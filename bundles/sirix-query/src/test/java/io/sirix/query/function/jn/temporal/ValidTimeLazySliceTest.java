package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.query.json.JsonDBItem;
import java.time.Instant;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeLazySliceTest {
  private static final String POINT = "xs:dateTime('2024-01-01T00:00:00Z')";
  private static final String TRANSACTION = "xs:dateTime('2099-01-01T00:00:00Z')";
  private static final String ROWS = """
      [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2024-01-01T00:00:00Z"},
       {"id":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
       {"id":3,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z",
         "nested":{"id":99,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}},
       {"id":4,"vf":"2024-01-01T00:00:00Z","vt":"2024-01-01T00:00:00Z"},
       {"id":5,"vf":"2025-01-01T00:00:00Z","vt":"2026-01-01T00:00:00Z"},
       {"id":7,"vf":"2023-01-01T00:00:00Z","vt":"2024-01-01T00:00:00.000500Z"},
       {"id":8,"vf":"2024-01-01T00:00:00.000500Z","vt":"2025-01-01T00:00:00Z"},
       {"id":9,"vf":"2023-01-01T00:00:00Z"},
       {"id":10,"vt":"2025-01-01T00:00:00Z"}]
      """;

  @TempDir
  Path directory;

  @Test
  void bothDirectionsStrictAndInclusiveAndMirrorsHaveExactPlansAndAnswers() {
    create(ROWS);
    final String[] comparisons = {POINT + " lt xs:dateTime($x.vt)", "xs:dateTime($x.vt) gt " + POINT,
        POINT + " < xs:dateTime($x.vt)", "xs:dateTime($x.vt) > " + POINT, POINT + " le xs:dateTime($x.vt)",
        "xs:dateTime($x.vt) ge " + POINT, "xs:dateTime($x.vf) lt " + POINT, POINT + " gt xs:dateTime($x.vf)",
        "xs:dateTime($x.vf) le " + POINT, POINT + " ge xs:dateTime($x.vf)"};
    final List<List<Long>> expected =
        List.of(List.of(2L, 3L, 7L, 10L), List.of(2L, 3L, 7L, 10L), List.of(2L, 3L, 7L, 10L), List.of(2L, 3L, 7L, 10L),
            List.of(1L, 2L, 3L, 4L, 7L, 10L), List.of(1L, 2L, 3L, 4L, 7L, 10L), List.of(1L, 3L, 7L, 9L),
            List.of(1L, 3L, 7L, 9L), List.of(1L, 2L, 3L, 4L, 7L, 9L), List.of(1L, 2L, 3L, 4L, 7L, 9L));
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (int i = 0; i < comparisons.length; i++) {
        for (final String resource : List.of("indexed", "plain")) {
          final String text = "for $x in " + source(resource) + " where " + comparisons[i] + " return $x.id";
          final Query query = new Query(chain, text);
          assertTrue(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE), text);
          assertEquals(expected.get(i), values(query.execute(context)), text);
        }
      }
    }
  }

  @Test
  void positionalTypedAndAllowingEmptyBindingsRetainTheirOriginalSemantics() {
    create(ROWS);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final Query positional = new Query(chain, "for $x at $position in " + source("indexed") + " where " + POINT
          + " lt xs:dateTime($x.vt) return $position");
      assertFalse(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE));
      assertEquals(List.of(2L, 3L, 5L, 7L), values(positional.execute(context)));
      final Query typed = new Query(chain,
          "for $x as item() in " + source("indexed") + " where " + POINT + " lt xs:dateTime($x.vt) return $x.id");
      assertFalse(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE));
      assertEquals(List.of(2L, 3L, 7L, 10L), values(typed.execute(context)));
      final String future = "xs:dateTime('2100-01-01T00:00:00Z')";
      final Query empty = new Query(chain, "for $x allowing empty in jn:open-bitemporal('slice','indexed',"
          + TRANSACTION + "," + future + ") where " + future + " lt xs:dateTime($x.vt) return 1");
      assertFalse(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE));
      assertEquals(List.of(), values(empty.execute(context)));
    }
  }

  @Test
  void functionBodyDynamicResourceCorrelatedPointAndResidualConjunct() {
    create(ROWS);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final String text = """
          declare function local:slice($r as xs:string, $t as xs:dateTime, $p as xs:dateTime) {
            for $x in jn:open-bitemporal('slice',$r,$t,$p)
            where $p lt xs:dateTime($x.vt) and $x.id ne 3 return $x
          };
          for $r in ('indexed','plain')
          for $p in (xs:dateTime('2024-01-01T00:00:00Z'), xs:dateTime('2024-01-01T00:00:00.000500Z'))
          for $x in local:slice($r,xs:dateTime('2099-01-01T00:00:00Z'),$p) return $x.id
          """;
      final Query query = new Query(chain, text);
      assertTrue(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE));
      assertEquals(List.of(2L, 7L, 10L, 2L, 8L, 10L, 2L, 7L, 10L, 2L, 8L, 10L), values(query.execute(context)));
    }
  }

  @Test
  void strictPlainFlworRetainsBoundaryPredicateAndOtherCastsAreNotConsumed() {
    create(ROWS);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String lower : List.of("lt", "le")) {
        final String text = "for $x in jn:doc('slice','indexed')[] where xs:dateTime($x.vf) " + lower + " " + POINT
            + " and " + POINT + " lt xs:dateTime($x.vt) return $x.id";
        final Query query = new Query(chain, text);
        assertEquals(lower.equals("lt")
            ? List.of(3L, 7L)
            : List.of(2L, 3L, 7L), values(query.execute(context)));
      }
      final Query unsupported =
          new Query(chain, "for $x in " + source("indexed") + " where xs:string($x.vt) gt '2024' return $x.id");
      assertFalse(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE));
      assertEquals(List.of(1L, 2L, 3L, 4L, 7L, 10L), values(unsupported.execute(context)));
    }
  }

  @Test
  void duplicateBoundsWithoutATreeIntervalStillUseTheOriginalFieldLookup() {
    create("""
        [{"id":1,"vf":"invalid","vf":"2026-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        assertEquals(List.of(1L),
            values(new Query(chain,
                "for $x in " + source(resource) + " where " + POINT + " lt xs:dateTime($x.vt) return $x.id").execute(
                    context)));
        final Query malformed = new Query(chain,
            "for $x in " + source(resource) + " where xs:dateTime($x.vf) lt " + POINT + " return $x.id");
        assertThrows(QueryException.class, () -> values(malformed.execute(context)));
      }
    }
  }

  @Test
  void strictResidualDoesNotSuppressErrorsAtJavaOnlyLexicalBounds() {
    create("""
        [{"id":1,"vf":"2010-01-01T00:00:00Z","vt":"2016-12-31T23:59:60Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final String point = "xs:dateTime('2016-12-31T23:59:59Z')";
      for (final String resource : List.of("indexed", "plain")) {
        final Query query = new Query(chain, "for $x in jn:open-bitemporal('slice','" + resource + "'," + TRANSACTION
            + "," + point + ") where " + point + " lt xs:dateTime($x.vt) return $x.id");
        assertThrows(QueryException.class, () -> values(query.execute(context)));
      }
    }
  }

  @Test
  void malformedResidualStillRaisesCastErrorAndWrongConfiguredFieldFallsBack() {
    create("[{\"id\":1,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"bad\",\"other\":\"2025-01-01T00:00:00Z\"}]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final Query malformed = new Query(chain,
            "for $x in " + source(resource) + " where " + POINT + " lt xs:dateTime($x.vt) return $x.id");
        assertThrows(QueryException.class, () -> values(malformed.execute(context)));
        final Query other = new Query(chain,
            "for $x in " + source(resource) + " where " + POINT + " lt xs:dateTime($x.other) return $x.id");
        assertEquals(List.of(1L), values(other.execute(context)));
      }
    }
  }

  @Test
  void fullyExactPlainSlicesAdmitAllFourEndpointModes() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2024-01-01T00:00:00Z"},
         {"id":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":3,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String lower : List.of("lt", "le")) {
        for (final String upper : List.of("lt", "le")) {
          for (final boolean reversed : List.of(false, true)) {
            final String first = "xs:dateTime($x.vf) " + lower + " " + POINT;
            final String second = POINT + " " + upper + " xs:dateTime($x.vt)";
            final Query query = new Query(chain, "for $x in jn:doc('slice','indexed')[] where " + (reversed
                ? second + " and " + first
                : first + " and " + second) + " return $x.id");
            assertTrue(contains(chain.getOptimizedAST(), ScanValidTimeIndex.SCAN_VALID_TIME_INDEX));
            final List<Long> expected = new ArrayList<>();
            if (upper.equals("le"))
              expected.add(1L);
            if (lower.equals("le"))
              expected.add(2L);
            expected.add(3L);
            assertEquals(expected, values(query.execute(context)));
          }
        }
      }
    }
  }

  @Test
  void precisionMetadataChangesEvenWhenRoundedEndpointsDoNotAndOldRevisionStaysExact() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2024-01-01T00:00:00.000500Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final var collection = store.lookup("slice");
      final JsonDBItem oldDocument = collection.getDocument("indexed");
      final int oldRevision = oldDocument.getTrx().getRevisionNumber();
      final var trx = oldDocument.getTrx();
      trx.moveToFirstChild();
      trx.moveToFirstChild();
      trx.moveToRightSibling();
      trx.moveToRightSibling();
      final long toKey = trx.getNodeKey();
      final var session = oldDocument.getResourceSession();
      final JsonNodeTrx writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
      writer.moveTo(toKey);
      writer.setStringValue("2024-01-01T00:00:00Z");
      writer.commit();
      final Instant point = Instant.parse("2024-01-01T00:00:00Z");
      assertEquals(1, ValidTimeIntervalIndex.keys(collection.getDocument("indexed", oldRevision), point, true).length);
      assertEquals(0, ValidTimeIntervalIndex.keys(collection.getDocument("indexed"), point, true).length);
    }
  }

  @Test
  void plainFallbackDoesNotSuppressMalformedCastsOutsideCandidateSet() {
    create("""
        [{"id":1,"vf":"invalid","vt":"invalid"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final Query query = new Query(chain, "for $x in jn:doc('slice','" + resource
            + "')[] where xs:dateTime($x.vf) le " + POINT + " and " + POINT + " lt xs:dateTime($x.vt) return $x.id");
        assertThrows(QueryException.class, () -> values(query.execute(context)));
      }
    }
  }

  @Test
  void reorderedArrayUsesOriginalOrderAndOldRevisionKeepsItsOrder() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":2,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final JsonDBItem doc = store.lookup("slice").getDocument("indexed");
      final int revision = doc.getTrx().getRevisionNumber();
      final long arrayKey = doc.getNodeKey();
      doc.getTrx().moveToFirstChild();
      doc.getTrx().moveToRightSibling();
      final long second = doc.getTrx().getNodeKey();
      final JsonNodeTrx writer =
          doc.getResourceSession().getNodeTrx().orElseGet(doc.getResourceSession()::beginNodeTrx);
      writer.moveTo(arrayKey);
      writer.moveSubtreeToFirstChild(second);
      writer.commit();
      final String predicate =
          "[] where xs:dateTime($x.vf) le " + POINT + " and " + POINT + " lt xs:dateTime($x.vt) return $x.id";
      assertEquals(List.of(2L, 1L),
          values(new Query(chain, "for $x in jn:doc('slice','indexed')" + predicate).execute(context)));
      assertEquals(List.of(1L, 2L), values(
          new Query(chain, "for $x in jn:doc('slice','indexed'," + revision + ")" + predicate).execute(context)));
    }
  }

  private void create(final String json) {
    final Path databasePath = directory.resolve("slice");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      for (final String name : List.of("indexed", "plain")) {
        database.createResource(ResourceConfiguration.newBuilder(name).validTimePaths("vf", "vt").build());
        try (var session = database.beginResourceSession(name); var trx = session.beginNodeTrx()) {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          trx.commit();
        }
      }
    }
  }

  private static void index(final SirixCompileChain chain, final SirixQueryContext context) {
    new Query(chain,
        "let $d := jn:doc('slice','indexed') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
            context);
  }

  private static String source(final String resource) {
    return "jn:open-bitemporal('slice','" + resource + "'," + TRANSACTION + "," + POINT + ")";
  }

  private static List<Long> values(final Sequence sequence) {
    final List<Long> values = new ArrayList<>();
    if (sequence != null) {
      try (Iter iterator = sequence.iterate()) {
        Item item;
        while ((item = iterator.next()) != null) {
          values.add(((Numeric) item).longValue());
        }
      }
    }
    return values;
  }

  private static boolean contains(final AST ast, final QNm function) {
    if (ast.getType() == XQ.FunctionCall && function.equals(ast.getValue())) {
      return true;
    }
    for (int i = 0; i < ast.getChildCount(); i++) {
      if (contains(ast.getChild(i), function)) {
        return true;
      }
    }
    return false;
  }
}
