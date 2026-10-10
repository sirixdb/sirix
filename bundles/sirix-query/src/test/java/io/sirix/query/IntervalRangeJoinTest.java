package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.ErrorCode;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.DTD;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.bench.bitemporal.BitemporalProjections;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Exact overlap comparisons, paired arithmetic and distinct counts after cold revision reopen. */
@Isolated
final class IntervalRangeJoinTest {
  @TempDir
  Path directory;

  private static final String CONTRACTS = """
      [{"id":1,"pid":10,"cost":4,"vf":"2024-01-01T00:00:00Z","vt":"2024-04-01T00:00:00Z"},
       {"id":1,"pid":10,"cost":7,"vf":"2024-04-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
       {"id":2,"pid":20,"cost":9,"vf":"2024-07-01T00:00:00Z","vt":"2024-10-01T00:00:00Z"},
       {"id":3,"pid":20,"cost":2,"vf":"2023-10-01T00:00:00Z","vt":"2024-01-01T00:00:00Z"}]
      """;
  private static final String PRODUCTS = """
      [{"id":10,"category":1,"retail":20,"vf":"2024-01-01T00:00:00Z","vt":"2024-04-01T00:00:00Z"},
       {"id":10,"category":1,"retail":30,"vf":"2024-04-01T00:00:00Z","vt":"2024-10-01T00:00:00Z"},
       {"id":20,"category":2,"retail":40,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
      """;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void timestampRowJoinsRetainColumnarUtcOutputInBothDirections(final VersioningType versioning) throws Exception {
    build(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String prolog = "declare variable $B := xs:dateTime('2024-01-15T00:00:00Z');"
          + "declare variable $P := xs:dateTime('2024-04-15T00:00:00Z');";
      final String contracts = "for $c in jn:open-bitemporal('range','contracts',$B,$P) ";
      final String products = "for $p in jn:open-bitemporal('range','products',$B,$P) ";
      final String tail = """
          where $c.pid eq $p.id
          and xs:dateTime($c.vf) lt xs:dateTime($p.vt)
          and xs:dateTime($p.vf) lt xs:dateTime($c.vt)
          order by $c.vf, $p.vt, $c.id
          return {'from':$c.vf,'to':$p.vt,'id':$c.id,'category':$p.category}
          """;
      for (final String sources : new String[] {contracts + products, products + contracts}) {
        final String query = prolog + sources + tail;
        final String expected = run(generic, ctx, query);
        final long before = SirixVectorizedExecutor.joinGroupServedCount();
        assertEquals(expected, run(chain, ctx, query));
        assertEquals(before + 1, SirixVectorizedExecutor.joinGroupServedCount());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void eagerRangeProbeDeclinesBoundErrorsForTheOriginalShortCircuitComparisons(final VersioningType versioning) {
    build(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      final JsonDBCollection collection = store.lookup("range");
      final var source = collection.getDocument("contracts");
      final var config = source.getResourceSession().getResourceConfig().getValidTimeConfig();
      final DateTime low = new DateTime("2024-01-01T00:00:00Z");
      final DateTime high = new DateTime("2024-07-01T00:00:00Z");
      assertNotNull(ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> high, () -> low, config, true, true));
      assertNull(ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> {
        throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "upper bound probe");
      }, () -> low, config, true, true));
      assertNull(ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> high, () -> {
        throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "lower bound probe");
      }, config, true, true));
      final DateTime invalidOffset = new DateTime((short) 2024, (byte) 4, (byte) 1, (byte) 0, (byte) 0, 0,
          new DTD(false, 1, (byte) 0, (byte) 0, 0));
      assertNull(
          ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> invalidOffset, () -> low, config, true, true));
      assertNull(
          ValidTimeIntervalIndex.comparisonRangeSequence(source, () -> high, () -> invalidOffset, config, true, true));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void joinedOverlapIsServedAndOracleExactAtBothTiesAndHistoricalRevisions(final VersioningType versioning)
      throws Exception {
    build(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String tx : new String[] {"2024-01-15T00:00:00Z", "2024-09-01T00:00:00Z", "2024-12-01T00:00:00Z"}) {
        for (final String low : new String[] {"2024-01-01T00:00:00Z", "2024-04-01T00:00:00Z", "2024-07-01T00:00:00Z"}) {
          final String prolog =
              "declare variable $B := xs:dateTime('" + tx + "');" + "declare variable $L := xs:dateTime('" + low + "');"
                  + "declare variable $U := xs:dateTime('2024-07-01T00:00:00Z');";
          for (final String start : new String[] {"lt", "le"}) {
            for (final String end : new String[] {"lt", "le"}) {
              final String query = prolog + joined(start, end);
              final String expected = run(generic, ctx, query);
              final long before = SirixVectorizedExecutor.joinGroupServedCount();
              assertEquals(expected, run(chain, ctx, query));
              assertEquals(before + 1, SirixVectorizedExecutor.joinGroupServedCount(), "overlap join must use columns");
              if (start.equals("lt") && end.equals("lt")) {
                for (final String operand : new String[] {"$c.pid", "$margin"}) {
                  final String distinct =
                      query.replace("count(distinct-values($cid))", "count(distinct-values(" + operand + "))");
                  final long served = SirixVectorizedExecutor.joinGroupServedCount();
                  assertEquals(run(generic, ctx, distinct), run(chain, ctx, distinct));
                  assertEquals(served + 1, SirixVectorizedExecutor.joinGroupServedCount(),
                      "sparse/extreme and computed distinct domains retain exact hash lanes");
                }
              }
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void denseDistinctExtremaPreservePairsTiesAndOverflow(final VersioningType versioning) throws Exception {
    final Path path = directory.resolve("range");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path)) {
      for (final String resource : new String[] {"contracts", "products"}) {
        db.createResource(ResourceConfiguration.newBuilder(resource)
                                               .validTimePaths("vf", "vt")
                                               .customCommitTimestamps(true)
                                               .versioningApproach(versioning)
                                               .storageType(StorageType.FILE_CHANNEL)
                                               .storeDiffs(false)
                                               .build());
        final String rows = resource.equals("contracts")
            ? """
                [{"id":1,"pid":10,"cost":-4,"vf":"2024-01-01T00:00:00Z","vt":"2024-04-01T00:00:00Z"},
                 {"id":1,"pid":10,"cost":7,"vf":"2024-04-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
                 {"id":2,"pid":20,"cost":9,"vf":"2024-01-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
                 {"id":3,"pid":99,"cost":0,"vf":"2024-01-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
                 {"id":4,"pid":10,"cost":10,"vf":"2024-01-01T00:00:00Z","vt":"2024-04-01T00:00:00Z"},
                 {"id":5,"pid":10,"cost":15,"vf":"2024-04-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
                 {"id":6,"pid":10,"cost":1,"vf":"2024-03-01T00:00:00Z","vt":"2024-05-01T00:00:00Z"}]
                """
            : PRODUCTS.replace("\"category\":1", "\"category\":-2")
                      .replace("\"category\":2", "\"category\":7")
                      .replace("]", ", {\"id\":11,\"category\":8,\"retail\":45,\"vf\":\"2024-01-01T00:00:00Z\","
                          + "\"vt\":\"2024-07-01T00:00:00Z\"}]");
        try (var session = db.beginResourceSession(resource); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(rows), JsonNodeTrx.Commit.NO);
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, "range");
          BitemporalProjections.declare(session, wtx, resource);
          wtx.commit("dense", Instant.parse("2024-01-15T00:00:00Z"));
          if (resource.equals("contracts")) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.moveToFirstChild();
            wtx.moveToFirstChild();
            wtx.moveToRightSibling();
            wtx.moveToRightSibling();
            wtx.moveToFirstChild();
            wtx.setNumberValue(Long.MIN_VALUE);
            wtx.commit("overflow", Instant.parse("2024-09-01T00:00:00Z"));
          }
        }
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String prolog = "declare variable $B := xs:dateTime('2024-01-15T00:00:00Z');"
          + "declare variable $L := xs:dateTime('2024-01-01T00:00:00Z');"
          + "declare variable $U := xs:dateTime('2024-07-01T00:00:00Z');";
      final String query = prolog + joined("lt", "lt");
      final String expected = "{\"category\":-2,\"contracts\":4,\"lo\":10,\"hi\":29} "
          + "{\"category\":7,\"contracts\":1,\"lo\":31,\"hi\":31}";
      assertEquals(expected, run(generic, ctx, query));
      for (final String body : new String[] {query, query.replace("$p.retail - $c.cost", "$c.cost - $p.retail"),
          query.replace("count(distinct-values($cid))", "count(distinct-values($p.id))"),
          query.replace("count(distinct-values($cid))", "count(distinct-values($c.cost))"),
          query.replace("count(distinct-values($cid))", "count(distinct-values($c.vf))")}) {
        final String oracle = run(generic, ctx, body);
        for (int repeat = 0; repeat < 2; repeat++) {
          final long before = SirixVectorizedExecutor.joinGroupServedCount();
          assertEquals(oracle, run(chain, ctx, body));
          assertEquals(before + 1, SirixVectorizedExecutor.joinGroupServedCount());
        }
      }
      final String overflow = query.replace("2024-01-15T00:00:00Z", "2024-09-01T00:00:00Z");
      final long before = SirixVectorizedExecutor.joinGroupServedCount();
      assertEquals(run(generic, ctx, overflow), run(chain, ctx, overflow));
      assertEquals(before, SirixVectorizedExecutor.joinGroupServedCount(), "overflow retains the scalar arithmetic");
      assertEquals(expected, run(chain, ctx, query), "historical columns survive the overflow fallback");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void overlapWindowsIntersectAlreadyRoutedPointSources(final VersioningType versioning) throws Exception {
    build(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String point : new String[] {"2024-04-01T00:00:00Z", "2024-07-01T00:00:00Z"}) {
        final String prolog =
            "declare variable $B := xs:dateTime('2024-01-15T00:00:00Z');" + "declare variable $P := xs:dateTime('"
                + point + "');" + "declare variable $L := xs:dateTime('2024-01-01T00:00:00Z');"
                + "declare variable $U := xs:dateTime('2024-07-01T00:00:00Z');";
        final String body = joined("lt", "lt")
                                              .replace("jn:open('range','contracts',$B)[]",
                                                  "jn:open-bitemporal('range','contracts',$B,$P)")
                                              .replace("jn:open('range','products',$B)[]",
                                                  "jn:open-bitemporal('range','products',$B,$P)");
        final long before = SirixVectorizedExecutor.joinGroupServedCount();
        final String expected = run(generic, ctx, prolog + body);
        assertEquals(point.startsWith("2024-04")
            ? "{\"category\":1,\"contracts\":1,\"lo\":23,\"hi\":23}"
            : "", expected);
        assertEquals(expected, run(chain, ctx, prolog + body));
        assertEquals(before + 1, SirixVectorizedExecutor.joinGroupServedCount());
      }
      final String prolog = "declare variable $B := xs:dateTime('2024-01-15T00:00:00Z');"
          + "declare variable $L := xs:dateTime('2024-01-01T00:00:00Z');"
          + "declare variable $U := xs:dateTime('2024-07-01T00:00:00Z');";
      final String textKeys =
          joined("lt", "lt").replace("$category := $p.category", "$category := $p.vt")
                            .replace("count(distinct-values($cid))", "count(distinct-values($c.vf))");
      final long before = SirixVectorizedExecutor.joinGroupServedCount();
      assertEquals(run(generic, ctx, prolog + textKeys), run(chain, ctx, prolog + textKeys));
      assertEquals(before + 1, SirixVectorizedExecutor.joinGroupServedCount(),
          "UTC timestamp keys preserve their original Z text");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void singleRangesKeepClosedStrictFractionalAndOutOfRangeAnswers(final VersioningType versioning) throws Exception {
    build(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String low : new String[] {"2024-01-01T00:00:00Z", "2024-04-01T00:00:00Z", "2024-07-01T00:00:00Z",
          "2024-04-01T00:00:00.000001Z", "0001-01-01T00:00:00Z", "12000-01-01T00:00:00Z"}) {
        for (final String high : new String[] {"2024-04-01T00:00:00Z", "2024-07-01T00:00:00Z",
            "12001-01-01T00:00:00Z"}) {
          for (final String start : new String[] {"lt", "le"}) {
            for (final String end : new String[] {"lt", "le"}) {
              final String query =
                  "declare variable $L := xs:dateTime('" + low + "');" + "declare variable $U := xs:dateTime('" + high
                      + "');" + "for $r in jn:doc('range','contracts',1)[] where xs:dateTime($r.vf) " + start
                      + " $U and $L " + end + " xs:dateTime($r.vt) return $r.id";
              assertEquals(run(generic, ctx, query), run(chain, ctx, query), query);
            }
          }
        }
      }
    }
  }

  private static String joined(final String start, final String end) {
    return """
        for $c in jn:open('range','contracts',$B)[]
        for $p in jn:open('range','products',$B)[]
        where $c.pid eq $p.id
          and xs:dateTime($c.vf) START $U and $L END xs:dateTime($c.vt)
          and xs:dateTime($p.vf) START $U and $L END xs:dateTime($p.vt)
          and xs:dateTime($c.vf) START xs:dateTime($p.vt)
          and xs:dateTime($p.vf) START xs:dateTime($c.vt)
        let $category := $p.category, $cid := $c.id, $margin := $p.retail - $c.cost
        group by $category
        let $contracts := count(distinct-values($cid)), $lo := min($margin), $hi := max($margin)
        order by $category
        return {"category":$category,"contracts":$contracts,"lo":$lo,"hi":$hi}
        """.replace("START", start).replace("END", end);
  }

  private void build(final VersioningType versioning) {
    final Path path = directory.resolve("range");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path)) {
      for (final String resource : new String[] {"contracts", "products"}) {
        db.createResource(ResourceConfiguration.newBuilder(resource)
                                               .validTimePaths("vf", "vt")
                                               .customCommitTimestamps(true)
                                               .versioningApproach(versioning)
                                               .storageType(StorageType.FILE_CHANNEL)
                                               .storeDiffs(false)
                                               .build());
        try (var session = db.beginResourceSession(resource); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(resource.equals("contracts")
              ? CONTRACTS
              : PRODUCTS), JsonNodeTrx.Commit.NO);
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, "range");
          BitemporalProjections.declare(session, wtx, resource);
          wtx.commit("first", Instant.parse("2024-01-15T00:00:00Z"));
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.moveToLastChild();
          wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(resource.equals("contracts")
              ? "{\"id\":4,\"pid\":10,\"cost\":11,\"vf\":\"2024-03-01T00:00:00Z\",\"vt\":\"2024-08-01T00:00:00Z\"}"
              : "{\"id\":20,\"category\":3,\"retail\":50,\"vf\":\"2024-04-01T00:00:00Z\",\"vt\":\"2024-07-01T00:00:00Z\"}"),
              JsonNodeTrx.Commit.NO);
          append(wtx, resource.equals("contracts")
              ? "{\"id\":5,\"pid\":-5,\"cost\":0,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-10-01T00:00:00Z\"}"
              : "{\"id\":-5,\"category\":0,\"retail\":8,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-10-01T00:00:00Z\"}");
          append(wtx, resource.equals("contracts")
              ? "{\"id\":6,\"pid\":9223372036854775807,\"cost\":2,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-10-01T00:00:00Z\"}"
              : "{\"id\":9223372036854775807,\"retail\":9,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-10-01T00:00:00Z\"}");
          wtx.commit("second", Instant.parse("2024-09-01T00:00:00Z"));
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          final long array = wtx.getNodeKey();
          wtx.moveToFirstChild();
          final long first = wtx.getNodeKey();
          moveToField(wtx, first, "vt");
          wtx.setStringValue("2024-05-01T00:00:00Z");
          if (resource.equals("contracts")) {
            moveToField(wtx, first, "cost");
            wtx.setNumberValue(13);
          } else {
            wtx.moveTo(array);
            wtx.moveToLastChild();
            wtx.remove();
          }
          wtx.commit("third", Instant.parse("2024-12-01T00:00:00Z"));
        }
      }
    }
  }

  private static void append(final JsonNodeTrx wtx, final String json) {
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.moveToLastChild();
    wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
  }

  private static void moveToField(final JsonNodeTrx wtx, final long object, final String field) {
    wtx.moveTo(object);
    if (!wtx.moveToFirstChild()) {
      throw new IllegalStateException("empty fixture object");
    }
    while (!field.equals(wtx.getName().getLocalName())) {
      if (!wtx.moveToRightSibling()) {
        throw new IllegalStateException("missing fixture field " + field);
      }
    }
    wtx.moveToFirstChild();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    try (var bytes = new ByteArrayOutputStream(); var writer = new PrintWriter(bytes, false, StandardCharsets.UTF_8)) {
      new Query(chain, query).serialize(ctx, writer);
      writer.flush();
      return bytes.toString(StandardCharsets.UTF_8).trim();
    }
  }
}
