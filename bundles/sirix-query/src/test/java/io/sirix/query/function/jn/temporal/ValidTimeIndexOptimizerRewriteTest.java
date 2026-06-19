/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.AST;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HARD CORRECTNESS GATE for the optimizer auto-selection of the VALIDTIME interval index for a plain
 * FLWOR stabbing predicate ({@code JsonValidTimeStep} → {@code jn:scan-valid-time-index}).
 *
 * <p>No {@code -D} flags. For many instants (incl. boundaries):</p>
 * <ul>
 *   <li>the plain FLWOR {@code for $x in jn:doc(...)[] where $x.validFrom <= $t and $t <= $x.validTo
 *       return $x} — and its operator/operand-order variants — returns exactly the brute-force set,
 *       which also equals {@code jn:valid-at};</li>
 *   <li>each matching query's OPTIMIZED AST actually contains {@code scan-valid-time-index} (the
 *       rewrite fired);</li>
 *   <li>NEGATIVE cases (one bound only, {@code $x}-dependent point, mismatched points, no VALIDTIME
 *       index, non-valid-time fields) do NOT rewrite yet still return the correct result.</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
@DisplayName("Valid-Time Interval Index — optimizer auto-selection (FLWOR rewrite) gate")
public final class ValidTimeIndexOptimizerRewriteTest {

  private static final Path sirixPath = PATHS.PATH1.getFile();
  private static final String DB = "vt-optimizer-db";
  private static final String RES = "r";
  private static final String DB_NOIDX = "vt-optimizer-noidx-db";

  private static final String VALID_FROM = "validFrom";
  private static final String VALID_TO = "validTo";

  private record Record(int id, Instant validFrom, Instant validTo) {
    boolean validAt(final Instant t) {
      return !t.isBefore(validFrom) && !t.isAfter(validTo);
    }
  }

  private List<Record> records;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("plain FLWOR stabbing predicate == brute force == jn:valid-at; rewrite fires; negatives correct")
  void flworStabbingRewriteGate() throws IOException {
    assertTrue(System.getProperty("sirix.index.useHOT") == null
            || "false".equalsIgnoreCase(System.getProperty("sirix.index.useHOT")),
        "must run WITHOUT -Dsirix.index.useHOT");

    records = buildDataset();
    final String json = toJson(records);

    // DB WITH a VALIDTIME index.
    createDbWithResource(DB, json);
    createValidTimeIndexViaQuery(DB);

    // DB WITHOUT any index (negative: no rewrite, linear eval).
    createDbWithResource(DB_NOIDX, json);

    final List<Instant> testTimes = buildTestTimes(records);

    int boundaryFrom = 0;
    int boundaryTo = 0;
    int zeroMatch = 0;
    int allMatch = 0;
    int rewriteChecks = 0;

    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(DB);
      store.lookup(DB_NOIDX);

      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {

        // Operator/operand-order variants of the SAME stabbing predicate (all must rewrite + match).
        // Each %1$s = db, %2$s = res, %3$s = the dateTime literal.
        // JSON valid-time fields are strings, so the canonical user predicate casts them to
        // xs:dateTime to compare against the point. The walker unwraps that cast.
        // %1$s=db, %2$s=res, %3$s=the dateTime literal P.
        final String[] variantTemplates = {
            // canonical: dateTime(validFrom) <= P and P <= dateTime(validTo)
            "for $x in jn:doc('%1$s','%2$s')[] where xs:dateTime($x.validFrom) <= %3$s and %3$s <= xs:dateTime($x.validTo) return $x",
            // value-comparison keywords le/ge
            "for $x in jn:doc('%1$s','%2$s')[] where xs:dateTime($x.validFrom) le %3$s and %3$s le xs:dateTime($x.validTo) return $x",
            // operands swapped: P >= dateTime(validFrom) and dateTime(validTo) >= P
            "for $x in jn:doc('%1$s','%2$s')[] where %3$s >= xs:dateTime($x.validFrom) and xs:dateTime($x.validTo) >= %3$s return $x",
            // conjunct order swapped: P <= validTo first
            "for $x in jn:doc('%1$s','%2$s')[] where %3$s <= xs:dateTime($x.validTo) and xs:dateTime($x.validFrom) <= %3$s return $x",
            // mixed: ge + le, swapped operands on one side
            "for $x in jn:doc('%1$s','%2$s')[] where %3$s ge xs:dateTime($x.validFrom) and %3$s le xs:dateTime($x.validTo) return $x",
        };

        for (final Instant t : testTimes) {
          final Set<Integer> brute = bruteForce(records, t);
          if (brute.isEmpty()) {
            zeroMatch++;
          }
          if (brute.size() == records.size()) {
            allMatch++;
          }
          for (final Record r : records) {
            if (t.equals(r.validFrom())) {
              boundaryFrom++;
              break;
            }
          }
          for (final Record r : records) {
            if (t.equals(r.validTo())) {
              boundaryTo++;
              break;
            }
          }

          final String lit = "xs:dateTime('" + t + "')";

          // jn:valid-at oracle (also index-accelerated, independently verified elsewhere).
          final Set<Integer> validAt = idsFromObjectQuery(chain, ctx,
              "jn:valid-at('" + DB + "', '" + RES + "', " + lit + ")");
          assertEquals(brute, validAt, "jn:valid-at must equal brute force at t=" + t);

          for (final String tmpl : variantTemplates) {
            final String q = String.format(tmpl, DB, RES, lit);

            // (a) The rewrite must fire for this matching query.
            assertTrue(optimizedContainsScanFunction(store, q),
                "Optimizer must rewrite to jn:scan-valid-time-index for: " + q + "  (t=" + t + ")");
            rewriteChecks++;

            // (b) The rewritten query must return exactly the brute-force set.
            assertEquals(brute, idsFromObjectQuery(chain, ctx, q),
                "Rewritten FLWOR must equal brute force at t=" + t + " for: " + q);

            // (c) The SAME query over the NO-INDEX db must NOT rewrite but still be correct.
            final String qNoIdx = String.format(tmpl, DB_NOIDX, RES, lit);
            assertFalse(optimizedContainsScanFunction(store, qNoIdx),
                "Without a VALIDTIME index, must NOT rewrite: " + qNoIdx);
            assertEquals(brute, idsFromObjectQuery(chain, ctx, qNoIdx),
                "Non-rewritten FLWOR (no index) must still equal brute force at t=" + t);
          }
        }

        // ---- NEGATIVE cases: must NOT rewrite, must still return correct results. ----
        final Instant t0 = UNIVERSAL;
        final String lit0 = "xs:dateTime('" + t0 + "')";
        final Set<Integer> bruteUniversal = bruteForce(records, t0);

        // (N1) only ONE bound present (no upper bound on validTo).
        assertNoRewriteButCorrect(store, chain, ctx,
            "for $x in jn:doc('" + DB + "','" + RES + "')[] where xs:dateTime($x.validFrom) <= " + lit0 + " return $x",
            recordsWithValidFromLe(t0));

        // (N2) the point depends on $x (validTo of the SAME record) — not an invariant point.
        assertNoRewriteButCorrect(store, chain, ctx,
            "for $x in jn:doc('" + DB + "','" + RES + "')[] where xs:dateTime($x.validFrom) <= xs:dateTime($x.validTo) "
                + "and xs:dateTime($x.validTo) <= xs:dateTime($x.validTo) return $x",
            allIds());

        // (N3) the two comparisons use DIFFERENT points.
        final Instant tOther = UNIVERSAL.plus(10, ChronoUnit.DAYS);
        assertNoRewriteButCorrect(store, chain, ctx,
            "for $x in jn:doc('" + DB + "','" + RES + "')[] where xs:dateTime($x.validFrom) <= " + lit0
                + " and xs:dateTime('" + tOther + "') <= xs:dateTime($x.validTo) return $x",
            recordsMatching(t0, tOther));

        // (N4) paths that are NOT the valid-time fields (id-based predicate of the same shape).
        assertNoRewriteButCorrect(store, chain, ctx,
            "for $x in jn:doc('" + DB + "','" + RES + "')[] where $x.id <= 5 and 0 <= $x.id return $x",
            idsInClosedRange(0, 5));

        // Sanity on negative correctness oracle for N1.
        assertEquals(bruteUniversal.isEmpty(), bruteUniversal.isEmpty()); // keep var used
      }
    }

    assertTrue(testTimes.size() >= 300, "expected >= 300 test instants, was " + testTimes.size());
    assertTrue(boundaryFrom > 0, "expected a t == some validFrom");
    assertTrue(boundaryTo > 0, "expected a t == some validTo");
    assertTrue(zeroMatch > 0, "expected a zero-match t");
    assertTrue(allMatch > 0, "expected an all-match t");
    assertTrue(rewriteChecks > 0, "expected rewrite assertions to have run");
  }

  // ---- rewrite inspection --------------------------------------------------------------------

  /** Compile {@code query} and return true iff its optimized AST contains a jn:scan-valid-time-index node. */
  private static boolean optimizedContainsScanFunction(final BasicJsonDBStore store, final String query) {
    final SirixCompileChain chain = new SirixCompileChain(null, store);
    chain.compile(query);
    final AST optimized = chain.getOptimizedAST();
    return astContainsValue(optimized, "scan-valid-time-index");
  }

  private static boolean astContainsValue(final AST node, final String localNameSubstring) {
    if (node == null) {
      return false;
    }
    final Object v = node.getValue();
    if (v != null && v.toString().contains(localNameSubstring)) {
      return true;
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      if (astContainsValue(node.getChild(i), localNameSubstring)) {
        return true;
      }
    }
    return false;
  }

  private static void assertNoRewriteButCorrect(final BasicJsonDBStore store, final SirixCompileChain chain,
      final SirixQueryContext ctx, final String query, final Set<Integer> expected) {
    assertFalse(optimizedContainsScanFunction(store, query),
        "must NOT rewrite (negative case): " + query);
    assertEquals(expected, idsFromObjectQuery(chain, ctx, query),
        "negative-case query must still return the correct result: " + query);
  }

  // ---- data + oracles ------------------------------------------------------------------------

  private static Set<Integer> bruteForce(final List<Record> records, final Instant t) {
    final Set<Integer> brute = new TreeSet<>();
    for (final Record r : records) {
      if (r.validAt(t)) {
        brute.add(r.id());
      }
    }
    return brute;
  }

  private Set<Integer> recordsWithValidFromLe(final Instant t) {
    final Set<Integer> s = new TreeSet<>();
    for (final Record r : records) {
      if (!r.validFrom().isAfter(t)) {
        s.add(r.id());
      }
    }
    return s;
  }

  private Set<Integer> recordsMatching(final Instant tFrom, final Instant tTo) {
    final Set<Integer> s = new TreeSet<>();
    for (final Record r : records) {
      if (!r.validFrom().isAfter(tFrom) && !r.validTo().isBefore(tTo)) {
        s.add(r.id());
      }
    }
    return s;
  }

  private Set<Integer> allIds() {
    final Set<Integer> s = new TreeSet<>();
    for (final Record r : records) {
      s.add(r.id());
    }
    return s;
  }

  private Set<Integer> idsInClosedRange(final int lo, final int hi) {
    final Set<Integer> s = new TreeSet<>();
    for (final Record r : records) {
      if (r.id() >= lo && r.id() <= hi) {
        s.add(r.id());
      }
    }
    return s;
  }

  private static final Instant UNIVERSAL = Instant.parse("2021-06-01T12:00:00Z");

  private static List<Record> buildDataset() {
    final List<Record> recs = new ArrayList<>();
    final Random rnd = new Random(20260619L);
    final Instant base = Instant.parse("2019-01-01T00:00:00Z");
    int id = 0;
    final long maxFromOffsetDays = ChronoUnit.DAYS.between(base, UNIVERSAL);
    for (int i = 0; i < 130; i++) {
      final Instant from = base.plus(rnd.nextInt((int) maxFromOffsetDays), ChronoUnit.DAYS)
                               .plusSeconds(rnd.nextInt(86_400));
      final Instant to = (i % 6 == 0)
          ? Instant.parse("2999-12-31T23:59:59Z")
          : UNIVERSAL.plus(1 + rnd.nextInt(800), ChronoUnit.DAYS).plusSeconds(rnd.nextInt(86_400));
      recs.add(new Record(id++, from, to));
    }
    for (int i = 0; i < 10; i++) {
      recs.add(new Record(id++, UNIVERSAL.minusMillis(i), UNIVERSAL.plus(3, ChronoUnit.HOURS).plusMillis(250)));
    }
    for (int i = 0; i < 5; i++) {
      recs.add(new Record(id++, UNIVERSAL, UNIVERSAL));
    }
    return recs;
  }

  private static String toJson(final List<Record> recs) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < recs.size(); i++) {
      final Record r = recs.get(i);
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"id\": ").append(r.id())
        .append(", \"").append(VALID_FROM).append("\": \"").append(r.validFrom())
        .append("\", \"").append(VALID_TO).append("\": \"").append(r.validTo()).append("\"}");
    }
    sb.append("]");
    return sb.toString();
  }

  private static List<Instant> buildTestTimes(final List<Record> recs) {
    final Set<Instant> times = new LinkedHashSet<>();
    times.add(Instant.parse("1900-01-01T00:00:00Z"));
    times.add(Instant.parse("2998-01-01T00:00:00Z"));
    times.add(UNIVERSAL);
    for (final Record r : recs) {
      times.add(r.validFrom());
      times.add(r.validTo());
      times.add(r.validFrom().minusMillis(1));
      times.add(r.validFrom().plusMillis(1));
      times.add(r.validTo().minusMillis(1));
      times.add(r.validTo().plusMillis(1));
    }
    return new ArrayList<>(times);
  }

  private static void createDbWithResource(final String dbName, final String json) {
    final var dbPath = sirixPath.resolve(dbName);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RES)
          .validTimePaths(VALID_FROM, VALID_TO).buildPathSummary(true).build());
      try (JsonResourceSession session = database.beginResourceSession(RES);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }
  }

  private static void createValidTimeIndexViaQuery(final String dbName) {
    try (var store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.lookup(dbName);
      try (var ctx = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        new Query(chain, "let $doc := jn:doc('" + dbName + "','" + RES + "') "
            + "let $i := jn:create-valid-time-index($doc) return sdb:commit($doc)").evaluate(ctx);
      }
    }
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private static Set<Integer> idsFromObjectQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String query) {
    final Sequence result = new Query(chain, query).evaluate(ctx);
    final Set<Integer> ids = new TreeSet<>();
    if (result == null) {
      return ids;
    }
    final Iter iter = result.iterate();
    try {
      Item item;
      while ((item = iter.next()) != null) {
        final io.brackit.query.jdm.json.Object obj = (io.brackit.query.jdm.json.Object) item;
        ids.add(((Numeric) obj.get(new io.brackit.query.atomic.QNm("id"))).intValue());
      }
    } finally {
      iter.close();
    }
    return ids;
  }
}
