package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
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
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
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
  /** A second database whose {@code suppliers} lack one region: a sparse hashed key column. */
  private static final String GAP_DB = "btest-gap";
  private static final String RES = BitemporalSchema.CONTRACTS;
  private static final int ROWS = 2_600; // three projection leaves, so masks and leaf pruning span leaves
  private static final Instant T0 = Instant.parse("2024-01-15T00:00:00Z");
  private static final Instant T1 = Instant.parse("2024-04-01T00:00:00Z");
  private static final Instant T2 = Instant.parse("2024-09-01T00:00:00Z");

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void literalSelectorsNeverAliasNestedProjectionColumns(final VersioningType versioning) throws Exception {
    buildAdmissionRegressionResources(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String opener = "jn:open-bitemporal('" + DB + "','aliases',$T,$P)";
      final String declarations = prolog(T0.toString(), "2024-02-01T00:00:00Z");
      final String pair = "for $a in " + opener + " for $b in " + opener + " where $a.id eq $b.id";
      final String grouped = pair + " let $grade := $a.grade, $v := $a.cost group by $grade"
          + " let $total := sum($v) order by $grade return {'grade':$grade,'v':$total}";
      final String rows = pair + " order by $a.id return {'id':$a.id,'v':$a.cost}";
      final long joined = SirixVectorizedExecutor.joinGroupServedCount();
      assertEquals(run(generic, ctx, declarations + grouped), run(chain, ctx, declarations + grouped));
      assertEquals(joined + 1, SirixVectorizedExecutor.joinGroupServedCount(), "ordinary joined grouping is served");
      assertEquals(run(generic, ctx, declarations + rows), run(chain, ctx, declarations + rows));
      assertEquals(joined + 2, SirixVectorizedExecutor.joinGroupServedCount(), "ordinary row join is served");
      for (final String side : new String[] {"$a", "$b"}) {
        final String literal = side + ".\"a/b\"";
        final String opposite = side.equals("$a")
            ? "$b"
            : "$a";
        final String joinCondition = literal + " eq " + opposite + ".cost";
        final String reported = grouped.replace("$v := $a.cost", "$v := " + literal);
        assertEquals("{\"grade\":0,\"v\":9}", run(generic, ctx, declarations + reported));
        for (final String body : new String[] {reported, grouped.replace("$grade := $a.grade", "$grade := " + literal),
            grouped.replace("$v := $a.cost", "$v := " + literal + " * " + side + ".qty"),
            grouped.replace("$v := $a.cost", "$v := " + side + ".qty * " + literal),
            grouped.replace("$a.id eq $b.id", joinCondition), rows.replace("$a.id eq $b.id", joinCondition),
            rows.replace("'v':$a.cost", "'v':" + literal), rows.replace("order by $a.id", "order by " + literal),
            rows.replace(" order by", " and " + literal + " eq " + opposite + ".cost order by"),
            rows.replace(" order by", " and " + literal + " ne " + opposite + ".cost order by")}) {
          final String query = declarations + body;
          final long before = SirixVectorizedExecutor.joinGroupServedCount();
          assertEquals(run(generic, ctx, query), run(chain, ctx, query), body);
          assertEquals(before, SirixVectorizedExecutor.joinGroupServedCount(),
              "literal join selector declines: " + body);
        }
      }
      final String single =
          "for $a in " + opener + " let $grade := $a.grade, $v := $a.cost * $a.qty group by $grade order by $grade"
              + " return {'grade':$grade,'v':sum($v)}";
      final long groupedBefore = SirixVectorizedExecutor.groupAggServedCount();
      assertEquals(run(generic, ctx, declarations + single), run(chain, ctx, declarations + single));
      assertEquals(groupedBefore + 1, SirixVectorizedExecutor.groupAggServedCount(),
          "ordinary computed grouping is served");
      for (final String body : new String[] {single.replace("$a.cost", "$a.\"a/b\""),
          single.replace("$a.qty", "$a.\"a/b\""),
          single.replace("$grade := $a.grade", "$grade := $a.id").replace("sum($v)", "sum($a.\"a/b\" * $a.qty)")}) {
        final long before = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(generic, ctx, declarations + body), run(chain, ctx, declarations + body));
        assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(), "literal computed operand declines");
      }
      for (final String quantifier : new String[] {"exists", "empty"}) {
        for (final String comparison : new String[] {"$b.\"a/b\" eq $a.cost", "$a.cost eq $b.\"a/b\"",
            "$b.cost eq $a.\"a/b\"", "$a.\"a/b\" eq $b.cost"}) {
          final String body = "let $new := " + opener + " for $a in " + opener + " where " + quantifier
              + "(for $b in $new where " + comparison + " return $b)"
              + " let $grade := $a.grade, $v := $a.cost group by $grade order by $grade"
              + " return {'grade':$grade,'v':sum($v)}";
          final long before = SirixVectorizedExecutor.groupAggServedCount();
          assertEquals(run(generic, ctx, declarations + body), run(chain, ctx, declarations + body), body);
          assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(), "literal membership selector declines");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void correlatedKeysAreEvaluatedOnlyForContributingOuterRows(final VersioningType versioning) throws Exception {
    buildAdmissionRegressionResources(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"aliases", "empty"}) {
        for (final String badField : new String[] {"epoch", "bucket"}) {
          final String opener = "jn:open-bitemporal('" + DB + "','" + resource + "',$T,xs:dateTime($e.ts))";
          final String driver = "[{\"epoch\":1,\"bucket\":2,\"ts\":\"2025-01-01T00:00:00Z\"}]".replace("\"" + badField
              + "\":" + (badField.equals("epoch")
                  ? 1
                  : 2),
              "\"" + badField + "\":\"bad\"");
          final String body = "for $e in DRIVER[] for $c in SRC"
              + " let $epoch := xs:integer($e.epoch), $bucket := xs:integer($e.bucket), $grade := $c.grade"
              + " group by $epoch,$bucket,$grade let $n := count($c) order by $epoch,$bucket,$grade"
              + " return {'epoch':$epoch,'bucket':$bucket,'grade':$grade,'n':$n}";
          final String declarations = prolog(T0.toString(), "2024-02-01T00:00:00Z");
          final String query = declarations + body.replace("DRIVER", driver).replace("SRC", opener);
          final long before = SirixVectorizedExecutor.groupAggServedCount();
          assertEquals("", run(generic, ctx, query));
          assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(), "empty reference stays generic");
          assertEquals("", run(chain, ctx, query));
          assertEquals(before + 1, SirixVectorizedExecutor.groupAggServedCount(),
              "empty correlated grouping is served");
          if (resource.equals("aliases")) {
            final String mixedDriver =
                driver.substring(0, driver.length() - 1) + ",{\"epoch\":1,\"bucket\":2,\"ts\":\"2024-02-01T00:00:00Z\"}"
                    + ",{\"epoch\":1,\"bucket\":2,\"ts\":\"2024-02-01T00:00:00Z\"}]";
            final String mixed = declarations + body.replace("DRIVER", mixedDriver).replace("SRC", opener);
            final long mixedBefore = SirixVectorizedExecutor.groupAggServedCount();
            final String expected = run(generic, ctx, mixed);
            assertEquals("{\"epoch\":1,\"bucket\":2,\"grade\":0,\"n\":4}", expected);
            assertEquals(expected, run(chain, ctx, mixed));
            assertEquals(mixedBefore + 3, SirixVectorizedExecutor.groupAggServedCount(),
                "empty and contributing tuples are served");
          }
        }
      }
    }
  }

  private void buildAdmissionRegressionResources(final VersioningType versioning) {
    final Path databasePath = directory.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      for (final String resource : new String[] {"aliases", "empty"}) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .validTimePaths("vf", "vt")
                                                     .customCommitTimestamps(true)
                                                     .buildPathSummary(true)
                                                     .versioningApproach(versioning)
                                                     .storeDiffs(false)
                                                     .build());
        try (JsonResourceSession session = database.beginResourceSession(resource);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          final String json = resource.equals("empty")
              ? "[]"
              : """
                  [{"id":1,"grade":0,"cost":7,"qty":2,"a/b":7,"a":{"b":3},
                    "vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
                   {"id":2,"grade":0,"cost":2,"qty":2,"a/b":2,"a":{"b":4},
                    "vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
                  """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
          final var paths = List.of(parse("/[]/id", PathParser.Type.JSON), parse("/[]/grade", PathParser.Type.JSON),
              parse("/[]/cost", PathParser.Type.JSON), parse("/[]/qty", PathParser.Type.JSON),
              parse("/[]/a/b", PathParser.Type.JSON), parse("/[]/vf", PathParser.Type.JSON),
              parse("/[]/vt", PathParser.Type.JSON));
          final IndexDef projection = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON), paths,
              List.of(Type.LON, Type.LON, Type.LON, Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projection), wtx);
          wtx.commit("S0", T0);
        }
      }
    }
  }

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
      // A hashed side whose group-key column has a gap (one supplier without a region): the gap row
      // carries no key, the sparse column's presence bit says so, and the served answer must match
      // the generic pipeline's empty-key group exactly.
      {
        final String gapQuery = prolog(txTimes.get(2), validTimes.get(1))
            + joinShapes().get(0)[1].replace("'" + DB + "','suppliers'", "'" + GAP_DB + "','suppliers'");
        final String gapReference = gapQuery.replace("jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P)",
            "(jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P))");
        final long served = SirixVectorizedExecutor.joinGroupServedCount();
        final String expected = run(chain, ctx, gapReference);
        assertEquals(expected, run(chain, ctx, gapQuery), "gap join versioning=" + versioning);
        assertEquals(served + 1, SirixVectorizedExecutor.joinGroupServedCount(),
            "a gap in the hashed key column is served through the presence bits");
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
      regressionShapes(chain, ctx, store, txTimes, validTimes);
    }
  }

  private static void regressionShapes(final SirixCompileChain chain, final SirixQueryContext ctx,
      final BasicJsonDBStore store, final List<String> txTimes, final List<String> validTimes) throws Exception {
    final String smallSource = "jn:open-bitemporal('" + DB + "','small-a',$T,$P)";
    final String smallProlog = prolog(txTimes.get(1), validTimes.get(1));
    fieldAggregateRegressions(chain, ctx, smallSource, smallProlog);
    joinRegressions(chain, ctx, smallSource, smallProlog, txTimes, validTimes);
    for (final String tx : txTimes) {
      for (final String valid : validTimes) {
        final String doc = "jn:open('" + DB + "','" + RES + "',$T)[]";
        final String body = shapes().get(0)[1];
        final String slice =
            "(for $r in " + doc + " where xs:dateTime($r.vf) le $P and $P lt xs:dateTime($r.vt) return $r)";
        final String query = prolog(tx, valid) + body.replace("SRC", slice);
        final String reference = prolog(tx, valid) + body.replace("SRC", "(" + source() + ")");
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(chain, ctx, reference), run(chain, ctx, query));
        assertEquals(served + (Instant.parse(tx).isBefore(T1)
            ? 1
            : 0), SirixVectorizedExecutor.groupAggServedCount(), "half-open slice serves only ordered arrays");
        final String foldedResidual = prolog(tx, valid)
            + body.replace("SRC", source()).replace("let $grade", "where $P lt xs:dateTime($c.vt) let $grade");
        final long residualServed = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(chain, ctx, reference), run(chain, ctx, foldedResidual));
        assertEquals(residualServed + 1, SirixVectorizedExecutor.groupAggServedCount(),
            "folded bitemporal slice is served");
      }
    }
    try (final SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final JsonResourceSession session = store.lookup(DB).getDatabase().beginResourceSession("small-a");
        final SirixCompileChain bound = SirixCompileChain.createWithJsonStore(store, session)) {
      for (final String valid : validTimes) {
        final String prolog = prolog(txTimes.get(0), valid);
        final String contracts = halfOpenSource(RES, "$T", "$P");
        final String suppliers = halfOpenSource("suppliers", "$T", "$P");
        for (final String[] shape : joinShapes()) {
          final String query =
              prolog + shape[1].replace(source(), contracts)
                               .replace("jn:open-bitemporal('" + DB + "','suppliers',$T,$P)", suppliers);
          final long served = SirixVectorizedExecutor.joinGroupServedCount();
          assertEquals(run(generic, ctx, query), run(chain, ctx, query));
          assertEquals(served + 1, SirixVectorizedExecutor.joinGroupServedCount(), "half-open join slices are served");
        }
        final String[] correlated = correlatedShapes().get(0);
        final String query = prolog + correlated[1].replace("SRC", halfOpenSource(RES, "$T", "$P"));
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(generic, ctx, query), run(chain, ctx, query));
        assertTrue(SirixVectorizedExecutor.groupAggServedCount() > served, "correlated half-open slice is served");
        final String scalar = query.replace("count($qty)", "count($grade)");
        final long scalarServed = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(generic, ctx, scalar), run(chain, ctx, scalar));
        assertEquals(scalarServed, SirixVectorizedExecutor.groupAggServedCount(), "correlated scalar key declines");
        final String scalarSum = query.replace("count($qty)", "sum($grade)");
        assertEquals(run(generic, ctx, scalarSum), run(chain, ctx, scalarSum));
        assertEquals(scalarServed, SirixVectorizedExecutor.groupAggServedCount(), "correlated scalar sum declines");
        final String scan = prolog
            + shapes().get(0)[1].replace("SRC", "jn:scan-valid-time-index(jn:open('" + DB + "','" + RES + "',$T),$P)");
        final long scanServed = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(generic, ctx, scan), run(chain, ctx, scan));
        assertEquals(scanServed + 1, SirixVectorizedExecutor.groupAggServedCount(),
            "public closed index scan is served");
      }
      ctx.bind(new QNm("cost"), new Str("qty"));
      final String dynamicSource = smallSource.replace("small-a", "dynamic");
      final String dynamicProlog = "declare variable $cost external;\n" + smallProlog;
      final String computed = "for $c in " + dynamicSource
          + " let $grade := $c.grade, $v := $c.$cost * $c.qty group by $grade order by $grade"
          + " return {'grade':$grade,'v':sum($v)}";
      assertEquals("{\"grade\":7,\"v\":9}", run(generic, ctx, dynamicProlog + computed));
      for (final String body : new String[] {computed, computed.replace("$v := $c.$cost * $c.qty", "$v := $c.$cost"),
          computed.replace("$grade := $c.grade", "$grade := $c.$cost"), computed.replace("sum($v)", "sum($c.$cost)"),
          computed.replace("sum($v)", "sum($c.$cost * $c.qty)")}) {
        final String query = dynamicProlog + body;
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(run(generic, ctx, query), run(chain, ctx, query));
        assertEquals(served, SirixVectorizedExecutor.groupAggServedCount(), "dynamic group selector declines");
      }
      final String pair = "for $a in " + dynamicSource + " for $b in " + dynamicSource.replace("dynamic", "small-b")
          + " where $a.id eq $b.id";
      final String groupedPair = pair + " let $grade := $a.grade, $v := $a.cost group by $grade order by $grade"
          + " return {'grade':$grade,'v':sum($v)}";
      final String rowsPair = pair + " order by $a.id return {'v':$a.cost}";
      for (final String body : new String[] {groupedPair.replace("$a.id eq $b.id", "$a.$cost eq $b.id"),
          groupedPair.replace("$v := $a.cost", "$v := $a.$cost"),
          groupedPair.replace("$v := $a.cost", "$v := $a.$cost * $a.qty"),
          rowsPair.replace("'v':$a.cost", "'v':$a.$cost"), rowsPair.replace("order by $a.id", "order by $a.$cost"),
          rowsPair.replace("order by", "and $a.$cost ne $b.cost order by")}) {
        final String query = dynamicProlog + body;
        final long served = SirixVectorizedExecutor.joinGroupServedCount();
        assertEquals(run(generic, ctx, query), run(chain, ctx, query));
        assertEquals(served, SirixVectorizedExecutor.joinGroupServedCount(), "dynamic join selector declines");
      }
      final String dependent = smallProlog + "for $e in jn:doc('" + DB + "','epochs')[] for $c in " + smallSource
          + " let $epoch := $e.epoch, $bucket := $epoch idiv 2, $grade := $c.grade, $cost := $c.cost"
          + " group by $epoch,$bucket,$grade let $n := count($cost) order by $epoch,$bucket,$grade"
          + " return {'epoch':$epoch,'bucket':$bucket,'grade':$grade,'n':$n}";
      final long dependentServed = SirixVectorizedExecutor.groupAggServedCount();
      assertEquals(run(generic, ctx, dependent), run(chain, ctx, dependent));
      assertEquals(dependentServed, SirixVectorizedExecutor.groupAggServedCount(), "dependent outer lets decline");
      final String body = shapes().get(0)[1];
      final String otherSource = smallSource.replace("small-a", "small-b");
      final String query = smallProlog + body.replace("SRC", otherSource);
      final long before = SirixVectorizedExecutor.groupAggServedCount();
      final String expected = run(chain, ctx, query.replace(otherSource, "(" + otherSource + ")"));
      assertEquals(expected, run(bound, ctx, query));
      assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(), "resource mismatch declines");
      for (final String cross : new String[] {"for $a in " + smallSource + " where exists(for $b in " + otherSource
          + " where $a.cost eq $b.cost return $b.id)"
          + " let $grade := $a.grade, $v := $a.cost group by $grade order by $grade return {'grade':$grade,'v':sum($v)}",
          "for $a in " + otherSource + " where exists(for $b in " + smallSource
              + " where $a.cost eq $b.cost return $b.id)"
              + " let $grade := $a.grade, $v := $a.cost group by $grade order by $grade return {'grade':$grade,'v':sum($v)}",
          "for $e in jn:doc('" + DB + "','epochs')[] for $c in " + otherSource
              + " let $epoch := $e.epoch, $grade := $c.grade, $v := $c.cost group by $epoch,$grade"
              + " let $total := sum($v) order by $epoch,$grade return {'epoch':$epoch,'grade':$grade,'total':$total}",
          "for $a in " + smallSource + " for $b in " + otherSource + " where $a.id eq $b.id"
              + " let $grade := $b.grade, $v := $b.cost group by $grade let $total := sum($v) order by $grade"
              + " return {'grade':$grade,'total':$total}",
          "for $a in " + otherSource + " for $b in " + smallSource + " where $a.id eq $b.id"
              + " let $grade := $a.grade, $v := $a.cost group by $grade let $total := sum($v) order by $grade"
              + " return {'grade':$grade,'total':$total}"}) {
        final String reference = smallProlog
            + cross.replace(smallSource, "(" + smallSource + ")").replace(otherSource, "(" + otherSource + ")");
        final long groups = SirixVectorizedExecutor.groupAggServedCount();
        final long joins = SirixVectorizedExecutor.joinGroupServedCount();
        assertEquals(run(chain, ctx, reference), run(bound, ctx, smallProlog + cross));
        assertEquals(groups, SirixVectorizedExecutor.groupAggServedCount(), "cross-resource grouping declines");
        assertEquals(joins, SirixVectorizedExecutor.joinGroupServedCount(), "cross-resource join declines");
      }
      final String same = smallProlog + body.replace("SRC", smallSource);
      assertEquals(run(chain, ctx, same.replace(smallSource, "(" + smallSource + ")")), run(bound, ctx, same));
      assertEquals(before + 1, SirixVectorizedExecutor.groupAggServedCount(), "bound resource still serves");
    }
  }

  private static String halfOpenSource(final String resource, final String tx, final String valid) {
    return "(for $r in jn:open('" + DB + "','" + resource + "'," + tx + ")[] where xs:dateTime($r.vf) le " + valid
        + " and " + valid + " lt xs:dateTime($r.vt) return $r)";
  }

  private static List<String[]> joinShapes() {
    final String contracts = "jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P)";
    final String suppliers = "jn:open-bitemporal('" + DB + "','suppliers',$T,$P)";
    return List.of(new String[] {"q9: two routed openers, long key from the hashed side", """
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
        new String[] {"duplicate join values on the hashed side, string key from the hashed side", """
            for $c in CONTRACTS
            for $s in SUPPLIERS
            where $c.grade eq $s.tier
            let $region := $s.region, $until := $s.vt, $qty := $c.qty
            group by $region, $until
            let $n := count($qty), $total := sum($qty)
            order by $region, $until
            return {"region":$region,"until":$until,"n":$n,"total":$total}
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
    final Path gapPath = directory.resolve(GAP_DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(gapPath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(gapPath)) {
      database.createResource(ResourceConfiguration.newBuilder("suppliers")
                                                   .validTimePaths("vf", "vt")
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("suppliers");
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(suppliers(true)), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, GAP_DB);
        BitemporalProjections.declare(session, wtx, "suppliers");
        wtx.commit("S0", T0);
      }
    }
    final Path databasePath = directory.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("counts")
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("counts");
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"qty\":3},{}]"), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        BitemporalProjections.declare(session, wtx, RES);
        wtx.commit();
      }
      for (final String resource : new String[] {"small-a", "small-b", "dynamic"}) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .validTimePaths("vf", "vt")
                                                     .customCommitTimestamps(true)
                                                     .buildPathSummary(true)
                                                     .versioningApproach(versioning)
                                                     .storeDiffs(false)
                                                     .build());
        try (JsonResourceSession session = database.beginResourceSession(resource);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          final int cost = resource.equals("small-b")
              ? 5
              : 2;
          final String row = """
              {"id":1,"pid":0,"sid":0,"cost":%d,"qty":3,"grade":7,
               "vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}
              """.formatted(cost).trim();
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + row + (resource.equals("dynamic")
              ? ""
              : "," + row.replace("\"id\":1", "\"id\":2")) + "]"), JsonNodeTrx.Commit.NO);
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
          BitemporalProjections.declare(session, wtx, RES);
          wtx.commit("S0", T0);
        }
      }
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
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(suppliers(false)), JsonNodeTrx.Commit.NO);
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

  /**
   * Twenty suppliers with the kit's integer region codes; two end their validity early, tiers repeat.
   * With {@code gap}, one supplier lacks its region: the hashed side's group-key column is sparse and
   * the column-side join must answer the generic pipeline's empty-key group exactly.
   */
  private static String suppliers(final boolean gap) {
    final StringBuilder json = new StringBuilder(2_048).append('[');
    for (int id = 0; id < 20; id++) {
      if (id > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(id);
      if (!gap || id != 3) {
        json.append(",\"region\":").append(id % 4);
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

  private static void fieldAggregateRegressions(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String smallSource, final String smallProlog) throws Exception {
    final String countSource = "jn:doc('" + DB + "','counts')[]";
    for (final String aggregate : new String[] {"count($q)", "xs:double(count($q))"}) {
      for (final boolean postGroup : new boolean[] {false, true}) {
        final String query = "for $c in " + countSource + " let $g := 1, $q := $c.qty group by $g" + (postGroup
            ? " let $n := " + aggregate
            : "") + " return {'n':"
            + (postGroup
                ? "$n"
                : aggregate)
            + ",'rows':count($c)}";
        final long before = SirixVectorizedExecutor.constGroupAggServedCount();
        final String expected = run(chain, ctx, query.replace(countSource, "(" + countSource + ")"));
        if (aggregate.equals("count($q)")) {
          assertEquals("{\"n\":1,\"rows\":2}", expected);
        }
        assertEquals(before, SirixVectorizedExecutor.constGroupAggServedCount(), "constant reference stays generic");
        assertEquals(expected, run(chain, ctx, query));
        assertEquals(before + 1, SirixVectorizedExecutor.constGroupAggServedCount(), "constant field count is served");
      }
    }
    for (final String aggregate : new String[] {"count($grade)", "sum($grade)"}) {
      final String query = smallProlog + "for $c in " + smallSource
          + " let $grade := $c.grade group by $grade order by $grade return {'grade':$grade,'n':" + aggregate + "}";
      final long before = SirixVectorizedExecutor.groupAggServedCount();
      assertEquals(aggregate.startsWith("count")
          ? "{\"grade\":7,\"n\":1}"
          : "{\"grade\":7,\"n\":7}", run(chain, ctx, query));
      assertEquals(before, SirixVectorizedExecutor.groupAggServedCount());
      final String postLet =
          query.replace(" order by", " let $n := " + aggregate + " order by").replace("'n':" + aggregate, "'n':$n");
      assertEquals(run(chain, ctx, query), run(chain, ctx, postLet));
      assertEquals(before, SirixVectorizedExecutor.groupAggServedCount());
    }
    for (final String key : new String[] {"$c.vf", "substring($c.vf,1,16)"}) {
      final String query = smallProlog + "for $c in " + smallSource + " let $k := " + key
          + ", $v := $c.cost * $c.qty group by $k order by $k return {'k':$k,'v':sum($v)}";
      final long before = SirixVectorizedExecutor.groupAggServedCount();
      assertEquals(run(chain, ctx, query.replace(smallSource, "(" + smallSource + ")")), run(chain, ctx, query));
      assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(), "unwired derived string lane declines");
    }
  }

  private static void joinRegressions(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String smallSource, final String smallProlog, final List<String> txTimes, final List<String> validTimes)
      throws Exception {
    final String positional = smallProlog + "for $c in " + smallSource + " for $s in jn:doc('" + DB
        + "','suppliers')[0] where $c.sid eq $s.id"
        + " let $grade := $c.grade, $v := $c.cost group by $grade let $n := count($v) order by $grade"
        + " return {'grade':$grade,'n':$n}";
    final long joined = SirixVectorizedExecutor.joinGroupServedCount();
    assertEquals(run(chain, ctx, positional.replace(smallSource, "(" + smallSource + ")")),
        run(chain, ctx, positional));
    assertEquals(joined, SirixVectorizedExecutor.joinGroupServedCount());
    final String scalarJoin = positional.replace("[0]", "[]").replace("count($v)", "count($grade)");
    assertEquals("{\"grade\":7,\"n\":1}", run(chain, ctx, scalarJoin));
    assertEquals(joined, SirixVectorizedExecutor.joinGroupServedCount());
    assertEquals("{\"grade\":7,\"n\":7}", run(chain, ctx, scalarJoin.replace("count($grade)", "sum($grade)")));
    assertEquals(joined, SirixVectorizedExecutor.joinGroupServedCount());
    for (final int width : new int[] {Long.SIZE, Long.SIZE + 1}) {
      final StringBuilder bindings = new StringBuilder();
      final StringBuilder keys = new StringBuilder();
      final StringBuilder returned = new StringBuilder();
      for (int key = 0; key < width; key++) {
        if (key > 0) {
          bindings.append(',');
          keys.append(',');
          returned.append(',');
        }
        bindings.append("$k")
                .append(key)
                .append(" := ")
                .append(key == Long.SIZE
                    ? "$s.region"
                    : "$c.grade");
        keys.append("$k").append(key);
        returned.append('\'')
                .append(key == 0
                    ? "grade"
                    : "k" + key)
                .append("':$k")
                .append(key);
      }
      final String wide = smallProlog + "for $c in " + smallSource + " for $s in jn:doc('" + GAP_DB
          + "','suppliers')[] where $c.qty eq $s.id let " + bindings + " group by " + keys
          + " let $n := count($c) order by " + keys + " return {" + returned + ",'n':$n}";
      final long beforeWide = SirixVectorizedExecutor.joinGroupServedCount();
      final String expected = run(chain, ctx, wide.replace(smallSource, "(" + smallSource + ")"));
      assertTrue(expected.startsWith("{\"grade\":7,"));
      assertTrue(expected.endsWith("\"n\":2}"));
      assertEquals(expected, run(chain, ctx, wide));
      assertEquals(beforeWide + (width == Long.SIZE
          ? 1
          : 0), SirixVectorizedExecutor.joinGroupServedCount(), "joined key-mask width " + width);
    }
    final String correction = "for $a in " + source().replace("$T", "xs:dateTime('" + txTimes.get(0) + "')")
        + " for $b in " + source().replace("$T", "xs:dateTime('" + txTimes.get(4) + "')")
        + " where $a.id eq $b.id and ($a.cost ne $b.cost or $a.qty ne $b.qty) order by $a.id"
        + " return {'id':$a.id,'old_cost':$a.cost,'new_cost':$b.cost,'old_qty':$a.qty,'new_qty':$b.qty}";
    for (final String valid : validTimes) {
      final String query = prolog(txTimes.get(0), valid) + correction;
      final String reference =
          query.replace("in jn:open-bitemporal(", "in (jn:open-bitemporal(").replace(",$P)", ",$P))");
      final long served = SirixVectorizedExecutor.joinGroupServedCount();
      assertEquals(run(chain, ctx, reference), run(chain, ctx, query));
      assertEquals(served + 1, SirixVectorizedExecutor.joinGroupServedCount(), "Q4 row join is served");
    }
  }

}
