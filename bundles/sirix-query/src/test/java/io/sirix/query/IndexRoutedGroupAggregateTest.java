package io.sirix.query;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.bench.bitemporal.BitemporalProjections;
import io.sirix.query.bench.bitemporal.BitemporalSchema;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Oracle-exact pinning of the INDEX-ROUTED grouped aggregates: a grouped FLWOR over
 * {@code jn:open-bitemporal('db','res', T, P)} served from the resource's projection under the
 * valid-time index's row mask must answer byte-for-byte what the generic pipeline answers, at every
 * revision (old ones included — T between two commits resolves to the earlier publication), at the
 * half-open valid-time boundaries ({@code vf} included, {@code vt} excluded), with missing fields,
 * and under all four versioning types.
 *
 * <p>
 * The reference is the same query with its source parenthesised: Brackit's walker cannot name a
 * path for that shape and the routing stage does not see a function call, so it compiles the
 * generic pipeline — the served counter proves which route each one took.
 * </p>
 */
final class IndexRoutedGroupAggregateTest {

  private static final String DB = "btest";
  private static final String RES = BitemporalSchema.CONTRACTS;
  private static final int ROWS = 2_600; // three projection leaves, so masks and leaf pruning span leaves
  private static final Instant T0 = Instant.parse("2024-01-15T00:00:00Z");
  private static final Instant T1 = Instant.parse("2024-04-01T00:00:00Z");
  private static final Instant T2 = Instant.parse("2024-09-01T00:00:00Z");

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void routedGroupsMatchTheGenericPipelineAtEveryRevisionAndBoundary(final VersioningType versioning) throws Exception {
    build(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final List<String> txTimes = List.of("2024-01-15T00:00:00Z", "2024-02-01T00:00:00Z", "2024-04-01T00:00:00Z",
          "2024-05-01T00:00:00Z", "2024-10-01T00:00:00Z");
      final List<String> validTimes = List.of("2024-01-01T00:00:00Z", "2024-02-15T00:00:00Z", "2024-03-01T00:00:00Z",
          "2024-05-31T23:59:59Z", "2024-06-01T00:00:00Z", "2024-12-31T00:00:00Z", "2025-01-01T00:00:00Z");
      int checked = 0;
      for (final String tx : txTimes) {
        for (final String valid : validTimes) {
          for (final String[] shape : shapes()) {
            final String routed = prolog(tx, valid) + shape[1].replace("SRC", source());
            final String reference = prolog(tx, valid) + shape[1].replace("SRC", "(" + source() + ")");
            final long before = SirixVectorizedExecutor.groupAggServedCount();
            final String expected = run(chain, ctx, reference);
            assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(),
                "the parenthesised reference must take the generic pipeline: " + shape[0]);
            final String actual = run(chain, ctx, routed);
            assertEquals(expected, actual, shape[0] + " tx=" + tx + " valid=" + valid + " versioning=" + versioning);
            assertEquals(before + 1, SirixVectorizedExecutor.groupAggServedCount(),
                "the routed query must be served from the projection: " + shape[0] + " tx=" + tx + " valid=" + valid);
            checked++;
          }
        }
      }
      assertTrue(checked >= txTimes.size() * validTimes.size() * shapes().size());
      // Without an order-by naming every key the routed grouping is NOT served: the opener's
      // record-key order and the projection's physical row order can differ at order exceptions.
      final String unordered = prolog(txTimes.get(2), validTimes.get(1)) + """
          for $c in SRC
          let $grade := $c.grade
          group by $grade
          return {"grade":$grade,"n":count($c)}
          """.replace("SRC", source());
      final long before = SirixVectorizedExecutor.groupAggServedCount();
      assertEquals(run(chain, ctx, unordered.replace(source(), "(" + source() + ")")), run(chain, ctx, unordered));
      assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(), "an unordered routed grouping stays generic");
      // MEMBERSHIP: an anti-join (Q12) and a semi-join against a second routed opener at another
      // revision, served as a row-key subtraction / intersection before the grouping.
      for (final boolean anti : new boolean[] {true, false}) {
        final String body = """
            declare variable $T2 := xs:dateTime('2024-10-01T00:00:00Z');
            let $new := jn:open-bitemporal('DBNAME','RESNAME',$T2,$P)
            for $a in SRC
            where QUANT(for $b in $new where $b.id eq $a.id return $b.id)
            let $grade := $a.grade, $value := $a.cost * $a.qty
            group by $grade
            let $n := count($value), $old_exposure := sum($value)
            order by $grade
            return {"grade":$grade,"n":$n,"old_exposure":$old_exposure}
            """.replace("DBNAME", DB)
               .replace("RESNAME", RES)
               .replace("QUANT", anti
                   ? "empty"
                   : "exists");
        final String routedQuery = prolog(txTimes.get(0), validTimes.get(1)) + body.replace("SRC", source());
        final String referenceQuery =
            prolog(txTimes.get(0), validTimes.get(1)) + body.replace("SRC", "(" + source() + ")");
        // (the $T2 declaration is the body's first line, which keeps it in the prolog)
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        final String expected = run(chain, ctx, referenceQuery);
        assertEquals(served, SirixVectorizedExecutor.groupAggServedCount(), "membership reference stays generic");
        assertEquals(expected, run(chain, ctx, routedQuery), "membership anti=" + anti + " versioning=" + versioning);
        assertEquals(served + 1, SirixVectorizedExecutor.groupAggServedCount(),
            "the membership grouping must be served: anti=" + anti);
      }
      // JOIN: a column-side equality join between two routed openers (SH1 Q9), between an opener and
      // a literal document, and with duplicate join values on the hashed side.
      for (final String[] shape : joinShapes()) {
        final String routedQuery = prolog(txTimes.get(2), validTimes.get(1)) + shape[1];
        final String referenceQuery = prolog(txTimes.get(2), validTimes.get(1))
            + shape[1].replace("jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P)",
                "(jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P))");
        final long served = SirixVectorizedExecutor.joinGroupServedCount();
        final String expected = run(chain, ctx, referenceQuery);
        assertEquals(served, SirixVectorizedExecutor.joinGroupServedCount(), "join reference stays generic");
        assertEquals(expected, run(chain, ctx, routedQuery), shape[0] + " versioning=" + versioning);
        assertEquals(served + 1, SirixVectorizedExecutor.joinGroupServedCount(),
            "the join grouping must be served: " + shape[0]);
      }
      // CORRELATED: an outer loop supplies the opener's instants and some keys (SH1 Q6/Q11); groups
      // of several outer tuples merge when an outer key repeats.
      for (final String[] shape : correlatedShapes()) {
        final String opener = source2().replace("TX", shape[2]).replace("VALID", shape[3]);
        final String routed = prolog(txTimes.get(2), validTimes.get(1)) + shape[1].replace("SRC", opener);
        final String reference =
            prolog(txTimes.get(2), validTimes.get(1)) + shape[1].replace("SRC", "(" + opener + ")");
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        final String expected = run(chain, ctx, reference);
        assertEquals(served, SirixVectorizedExecutor.groupAggServedCount(), "reference stays generic: " + shape[0]);
        assertEquals(expected, run(chain, ctx, routed), shape[0] + " versioning=" + versioning);
        assertTrue(SirixVectorizedExecutor.groupAggServedCount() > served,
            "the correlated grouping must be served per outer tuple: " + shape[0]);
      }
    }
  }

  private static List<String[]> joinShapes() {
    final String contracts = "jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P)";
    final String suppliers = "jn:open-bitemporal('" + DB + "','suppliers',$T,$P)";
    return List.of(new String[] {"q9: two routed openers, string key from the hashed side", """
        for $c in CONTRACTS
        for $s in SUPPLIERS
        where $c.sid eq $s.id
        let $region := $s.region, $grade := $c.grade, $value := $c.cost * $c.qty
        group by $region,$grade
        let $n := count($value), $exposure := sum($value)
        order by $region,$grade
        return {"region":$region,"grade":$grade,"n":$n,"exposure":$exposure}
        """.replace("CONTRACTS", contracts).replace("SUPPLIERS", suppliers)},
        new String[] {"opener joined with a literal document, pair count and extrema", """
            for $c in CONTRACTS
            for $s in jn:doc('DBNAME','suppliers')[]
            where $s.id = $c.sid
            let $tier := $s.tier, $grade := $c.grade, $cost := $c.cost
            group by $tier,$grade
            let $pairs := count($c), $lo := min($cost), $hi := max($cost)
            order by $grade, $tier descending
            return {"tier":$tier,"grade":$grade,"pairs":$pairs,"lo":$lo,"hi":$hi}
            """.replace("CONTRACTS", contracts).replace("DBNAME", DB)},
        new String[] {"duplicate join values on the hashed side", """
            for $c in CONTRACTS
            for $s in SUPPLIERS
            where $c.grade eq $s.tier
            let $region := $s.region, $qty := $c.qty
            group by $region
            let $n := count($qty), $total := sum($qty)
            order by $region
            return {"region":$region,"n":$n,"total":$total}
            """.replace("CONTRACTS", contracts).replace("SUPPLIERS", suppliers)});
  }

  /** The opener with placeholder instants; a shape supplies them (they may read the outer row). */
  private static String source2() {
    return "jn:open-bitemporal('" + DB + "','" + RES + "',TX,VALID)";
  }

  private static List<String[]> correlatedShapes() {
    return List.of(new String[] {"q6: per-epoch revision, outer key + inner key", """
        for $e in jn:doc('DBNAME','epochs')[]
        for $c in SRC
        let $epoch := $e.epoch, $grade := $c.grade, $qty := $c.qty
        group by $epoch,$grade
        let $n := count($qty), $qty_sum := sum($qty)
        order by $epoch,$grade
        return {"epoch":$epoch,"grade":$grade,"n":$n,"qty_sum":$qty_sum}
        """.replace("DBNAME", DB), "xs:dateTime($e.ts)", "$P"},
        new String[] {"q11: per-day valid instant with an outer where, computed sum", """
            for $d in jn:doc('DBNAME','days')[]
            where $d.day_no ge 2 and $d.day_no lt 8
            for $c in SRC
            let $day_no := $d.day_no, $grade := $c.grade, $value := $c.cost * $c.qty
            group by $day_no,$grade
            let $n := count($value), $exposure := sum($value)
            order by $day_no,$grade
            return {"day_no":$day_no,"grade":$grade,"n":$n,"exposure":$exposure}
            """.replace("DBNAME", DB), "$T", "xs:dateTime($d.ts)"},
        new String[] {"merged outer key: groups of several epochs fold together", """
            for $e in jn:doc('DBNAME','epochs')[]
            for $c in SRC
            let $bucket := $e.epoch idiv 2, $grade := $c.grade, $cost := $c.cost
            group by $bucket,$grade
            let $n := count($c), $lo := min($cost), $hi := max($cost), $total := sum($cost)
            order by $grade descending, $bucket
            return {"bucket":$bucket,"grade":$grade,"n":$n,"lo":$lo,"hi":$hi,"total":$total}
            """.replace("DBNAME", DB), "xs:dateTime($e.ts)", "$P"});
  }

  /** {@code {name, body}} pairs; {@code SRC} is the loop source. */
  private static List<String[]> shapes() {
    return List.of(new String[] {"q7: single key, count of a let, computed sum", """
        for $c in SRC
        let $grade := $c.grade, $qty := $c.qty, $value := $c.cost * $c.qty
        group by $grade
        let $n := count($qty), $qty_sum := sum($qty), $exposure := sum($value)
        order by $grade
        return {"grade":$grade,"n":$n,"qty_sum":$qty_sum,"exposure":$exposure}
        """}, new String[] {"q8: two keys, count/min/max", """
        for $c in SRC
        let $sid := $c.sid, $grade := $c.grade, $cost := $c.cost
        group by $sid,$grade
        let $n := count($cost), $min_cost := min($cost), $max_cost := max($cost)
        order by $sid,$grade
        return {"sid":$sid,"grade":$grade,"n":$n,"min_cost":$min_cost,"max_cost":$max_cost}
        """}, new String[] {"row count, avg and a where predicate", """
        for $c in SRC
        where $c.grade ge 2
        let $grade := $c.grade
        group by $grade
        let $n := count($c), $avg_cost := avg($c.cost), $qty := sum($c.qty)
        order by $grade
        return {"grade":$grade,"n":$n,"avg_cost":$avg_cost,"qty":$qty}
        """}, new String[] {"OR predicate: the row mask joins a predicate tree", """
        for $c in SRC
        where $c.grade eq 1 or $c.sid eq 3
        let $sid := $c.sid, $value := $c.cost * $c.qty
        group by $sid
        let $n := count($value), $total := sum($value), $rows := count($c)
        order by $sid
        return {"sid":$sid,"n":$n,"total":$total,"rows":$rows}
        """}, new String[] {"computed sum with a literal, no order by", """
        for $c in SRC
        let $sid := $c.sid, $margin := $c.cost * 3 - $c.qty
        group by $sid
        order by $sid
        return {"sid":$sid,"n":count($c),"margin":sum($margin),"low":min($margin)}
        """});
  }

  private static String prolog(final String tx, final String valid) {
    return "declare variable $T := xs:dateTime('" + tx + "');\ndeclare variable $P := xs:dateTime('" + valid + "');\n";
  }

  private static String source() {
    return "jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P)";
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream(); final PrintWriter pw = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, pw);
      pw.flush();
      return out.toString().trim();
    }
  }

  /**
   * Three publications: E0 declares the indexes over 2,600 segments (some without {@code qty}, some
   * with shifted bounds), E1 edits costs and bounds and appends new segments, E2 removes segments and
   * edits grades.
   */
  private void build(final VersioningType versioning) {
    final Path databasePath = directory.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      // The small driver tables of the correlated shapes.
      for (final String[] table : new String[][] {
          {"epochs", "[{\"epoch\":0,\"ts\":\"2024-01-15T00:00:00Z\"},{\"epoch\":1,\"ts\":\"2024-04-01T00:00:00Z\"},"
              + "{\"epoch\":2,\"ts\":\"2024-09-01T00:00:00Z\"}]"},
          {"days", days()}}) {
        database.createResource(ResourceConfiguration.newBuilder(table[0]).buildPathSummary(true).build());
        try (JsonResourceSession session = database.beginResourceSession(table[0]);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(table[1]), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
      database.createResource(ResourceConfiguration.newBuilder(RES)
                                                   .validTimePaths("vf", "vt")
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      database.createResource(ResourceConfiguration.newBuilder("suppliers")
                                                   .validTimePaths("vf", "vt")
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("suppliers");
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(suppliers()), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
        BitemporalProjections.declare(session, wtx, "suppliers");
        wtx.commit("S0", T0);
      }
      try (JsonResourceSession session = database.beginResourceSession(RES); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(rows()), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
        BitemporalProjections.declare(session, wtx, RES);
        wtx.commit("E0", T0);
      }
      try (JsonResourceSession session = database.beginResourceSession(RES); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        final long array = wtx.getNodeKey();
        // Edit every 13th segment's cost and every 17th segment's end, so E1 differs from E0 on the
        // rows the mask selects AND on the mask itself.
        wtx.moveToFirstChild();
        int i = 0;
        do {
          final long object = wtx.getNodeKey();
          if (i % 13 == 0) {
            setNumber(wtx, object, "cost", 100 + i);
          }
          if (i % 17 == 0) {
            setString(wtx, object, "vt", "2024-06-01T00:00:00Z");
          }
          i++;
        } while (wtx.moveTo(wtx.getNodeKey()) && wtx.moveToRightSibling());
        wtx.moveTo(array);
        // An ORDER EXCEPTION in the projection: a record stored before its siblings with a key above
        // theirs; every evaluator's membership walk must take the binary-search arm for it.
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(row(ROWS + 500, "2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z", true)),
            JsonNodeTrx.Commit.NO);
        wtx.moveTo(array);
        final StringBuilder appended = new StringBuilder();
        for (int k = 0; k < 60; k++) {
          wtx.moveTo(array);
          wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(
              row(ROWS + k, "2024-06-01T00:00:00Z", "2025-01-01T00:00:00Z", k % 9 != 0)), JsonNodeTrx.Commit.NO);
          appended.append(k);
        }
        wtx.commit("E1", T1);
      }
      try (JsonResourceSession session = database.beginResourceSession(RES); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        int i = 0;
        while (true) {
          final long object = wtx.getNodeKey();
          final boolean hasNext = wtx.moveToRightSibling();
          final long next = hasNext
              ? wtx.getNodeKey()
              : -1L;
          if (i % 19 == 0) {
            wtx.moveTo(object);
            wtx.remove();
          } else if (i % 7 == 0) {
            setNumber(wtx, object, "grade", (i / 7) % 4);
          }
          if (!hasNext) {
            break;
          }
          wtx.moveTo(next);
          i++;
        }
        wtx.commit("E2", T2);
      }
    }
  }

  private static void setNumber(final JsonNodeTrx wtx, final long object, final String field, final long value) {
    moveToFieldValue(wtx, object, field);
    wtx.setNumberValue(value);
  }

  private static void setString(final JsonNodeTrx wtx, final long object, final String field, final String value) {
    moveToFieldValue(wtx, object, field);
    wtx.setStringValue(value);
  }

  private static void moveToFieldValue(final JsonNodeTrx wtx, final long object, final String field) {
    wtx.moveTo(object);
    if (!wtx.moveToFirstChild()) {
      throw new IllegalStateException("empty object " + object);
    }
    while (!field.equals(wtx.getName().getLocalName())) {
      if (!wtx.moveToRightSibling()) {
        throw new IllegalStateException("object " + object + " has no field " + field);
      }
    }
    wtx.moveToFirstChild();
  }

  /** Ten days, twenty days apart from 2024-01-01: valid instants that cross the fixture's bounds. */
  private static String days() {
    final StringBuilder json = new StringBuilder(512).append('[');
    for (int day = 0; day < 10; day++) {
      if (day > 0) {
        json.append(',');
      }
      json.append("{\"day_no\":")
          .append(day)
          .append(",\"ts\":\"")
          .append(Instant.parse("2024-01-01T00:00:00Z").plusSeconds(day * 20L * 86_400L))
          .append("\"}");
    }
    return json.append(']').toString();
  }

  /** Twenty suppliers; one lacks its region, two end their validity early, tiers repeat. */
  private static String suppliers() {
    final StringBuilder json = new StringBuilder(2_048).append('[');
    for (int id = 0; id < 20; id++) {
      if (id > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(id);
      if (id != 3) {
        json.append(",\"region\":\"").append(new String[] {"north", "south", "east", "west"}[id % 4]).append('"');
      }
      json.append(",\"tier\":")
          .append(id % 4)
          .append(",\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"")
          .append(id % 9 == 8
              ? "2024-02-10T00:00:00Z"
              : "2025-01-01T00:00:00Z")
          .append("\"}");
    }
    return json.append(']').toString();
  }

  private static String rows() {
    final StringBuilder json = new StringBuilder(ROWS * 110).append('[');
    for (int i = 0; i < ROWS; i++) {
      if (i > 0) {
        json.append(',');
      }
      final String vf = i % 5 == 0
          ? "2024-03-01T00:00:00Z"
          : "2024-01-01T00:00:00Z";
      final String vt = i % 11 == 0
          ? "2024-06-01T00:00:00Z"
          : "2025-01-01T00:00:00Z";
      json.append(row(i, vf, vt, i % 97 != 0));
    }
    return json.append(']').toString();
  }

  private static String row(final int i, final String vf, final String vt, final boolean withQty) {
    final StringBuilder json = new StringBuilder(110);
    json.append("{\"id\":")
        .append(i + 1)
        .append(",\"pid\":")
        .append(i % 50)
        .append(",\"sid\":")
        .append(i % 7)
        .append(",\"cost\":")
        .append(1_000 + (i * 37) % 900);
    if (withQty) {
      json.append(",\"qty\":").append(1 + i % 23);
    }
    json.append(",\"grade\":")
        .append(i % 4)
        .append(",\"vf\":\"")
        .append(vf)
        .append("\",\"vt\":\"")
        .append(vt)
        .append("\"}");
    return json.toString();
  }
}
