package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.DateTime;
import io.sirix.query.json.JsonDBObject;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Una;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
  void duplicateBoundsSpanningTheDomainStillUseTheOriginalFieldLookup() {
    create("""
        [{"id":1,"vf":"invalid","vf":"2026-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":2,"vf":"invalid","vf":"2026-01-01T00:00:00Z","vt":"2030-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        // Strict and inclusive endpoints, and the plain closed point route, must all reach it: only
        // the strict routes union the verification postings, so the record's own registered
        // whole-domain interval is what the other two depend on.
        for (final String upper : List.of("lt", "le")) {
          assertEquals(List.of(1L, 2L), values(new Query(chain, "for $x in " + source(resource) + " where " + POINT + " "
              + upper + " xs:dateTime($x.vt) return $x.id").execute(context)), resource + " " + upper);
        }
        assertEquals(List.of(1L, 2L),
            values(new Query(chain,
                "for $x in jn:valid-at('slice','" + resource + "'," + POINT + ") return $x.id").execute(context)),
            resource);
        final Query malformed = new Query(chain,
            "for $x in " + source(resource) + " where xs:dateTime($x.vf) lt " + POINT + " return $x.id");
        assertThrows(QueryException.class, () -> values(malformed.execute(context)));
      }
    }
  }

  /**
   * A {@code validFrom} before the domain origin clamps to the domain minimum, so the strict-start
   * tie removal drops it at the origin even though it really starts earlier. Only the verification
   * union restores it, which is why a strict start needs that union just as a strict end does.
   */
  @Test
  void aClampedStartBoundStillMatchesAStrictStartSliceAtTheDomainOrigin() {
    create("""
        [{"id":1,"vf":"1969-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final String origin = "xs:dateTime('1970-01-01T00:00:00Z')";
      for (final String resource : List.of("indexed", "plain")) {
        final Query query = new Query(chain, "for $x in jn:open-bitemporal('slice','" + resource + "'," + TRANSACTION
            + "," + origin + ") where xs:dateTime($x.vf) lt " + origin + " return $x.id");
        assertTrue(contains(chain.getOptimizedAST(), OpenBitemporal.OPEN_BITEMPORAL_SLICE), resource);
        assertEquals(List.of(1L), values(query.execute(context)), resource);
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

  @Test
  void duplicateEndBoundsMatchBeyondTheFirstParseableEnd() {
    create("""
        [{"id":1,"vf":"2020-01-01T00:00:00Z","vt":"bad","vt":"2021-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        assertEquals(List.of(1L), values(new Query(chain,
            "for $x in jn:valid-at('slice','" + resource + "'," + POINT + ") return $x.id").execute(context)));
        assertEquals(List.of(1L), values(new Query(chain,
            "for $x in " + source(resource) + " where xs:dateTime($x.vf) lt " + POINT + " return $x.id").execute(context)));
        assertEquals(List.of(1L), values(new Query(chain,
            "for $x in jn:scan-valid-time-index(jn:doc('slice','" + resource + "')," + POINT
                + ") return $x.id").execute(context)));
      }
    }
  }

  @Test
  void incrementalDuplicateBoundsAndHistoricalReadsUseFirstOccurrences() {
    create("""
        [{"id":1,"vf":"2026-01-01T00:00:00Z","vt":"2030-01-01T00:00:00Z"},
         {"id":2,"vf":"2020-01-01T00:00:00Z","vt":"2021-01-01T00:00:00Z"}]
        """);
    final int oldRevision;
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      oldRevision = store.lookup("slice").getDocument("indexed").getTrx().getRevisionNumber();
      for (final String resource : List.of("indexed", "plain")) {
        final JsonDBItem document = store.lookup("slice").getDocument(resource);
        final var cursor = document.getTrx();
        cursor.moveTo(document.getNodeKey());
        cursor.moveToFirstChild();
        final long first = cursor.getNodeKey();
        cursor.moveToRightSibling();
        final long second = cursor.getNodeKey();
        final var session = document.getResourceSession();
        final JsonNodeTrx writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
        writer.moveTo(first);
        writer.insertObjectRecordAsFirstChild("vf", new StringValue("bad"));
        writer.moveTo(second);
        writer.insertObjectRecordAsFirstChild("vt", new StringValue("bad"));
        writer.commit();
        assertEquals(List.of(1L, 2L), values(new Query(chain,
            "for $x in jn:valid-at('slice','" + resource + "'," + POINT + ") return $x.id").execute(context)));
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      assertEquals(List.of(), values(new Query(chain,
          "for $x in jn:scan-valid-time-index(jn:doc('slice','indexed'," + oldRevision + ")," + POINT
              + ") return $x.id").execute(context)));
      for (final String resource : List.of("indexed", "plain")) {
        assertEquals(List.of(1L, 2L), values(new Query(chain,
            "for $x in jn:valid-at('slice','" + resource + "'," + POINT + ") return $x.id").execute(context)));
      }
    }
  }

  @Test
  void computedFieldsKeepTheirPerTupleValues() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z","other":"2022-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final String fields = "for $vt in ('vt','other') for $x in ";
        assertEquals(List.of(1L), values(new Query(chain, fields + source(resource)
            + " where " + POINT + " lt xs:dateTime($x.$vt) return $x.id").execute(context)));
        assertEquals(List.of(1L), values(new Query(chain, fields + "jn:doc('slice','" + resource
            + "')[] where xs:dateTime($x.vf) le " + POINT + " and " + POINT
            + " lt xs:dateTime($x.$vt) return $x.id").execute(context)));
      }
    }
  }

  @Test
  void timezoneLessPointsUseOrdinaryComparisonsForBothRoutesAndDirections() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2024-01-01T00:00:00Z"},
         {"id":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    final String point = "xs:dateTime('2024-01-01T00:00:00')";
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final String direct = source(resource).replace(POINT, point);
        for (final String operator : List.of("lt", "le")) {
          final String end = point + " " + operator + " xs:dateTime($x.vt)";
          final String start = "xs:dateTime($x.vf) " + operator + " " + point;
          assertEquals(List.of(1L, 2L), values(new Query(chain,
              "for $x in " + direct + " where " + end + " return $x.id").execute(context)));
          assertEquals(List.of(1L, 2L), values(new Query(chain,
              "for $x in " + direct + " where " + start + " return $x.id").execute(context)));
          assertEquals(List.of(1L, 2L), values(new Query(chain,
              "for $x in jn:doc('slice','" + resource + "')[] where " + start + " and " + end
                  + " return $x.id").execute(context)));
          final String mirror = operator.equals("lt") ? "gt" : "ge";
          assertEquals(List.of(2L), values(new Query(chain,
              "for $x in " + direct + " where xs:dateTime($x.vt) " + mirror + " " + point
                  + " return $x.id").execute(context)));
          assertEquals(List.of(1L), values(new Query(chain,
              "for $x in " + direct + " where " + point + " " + mirror
                  + " xs:dateTime($x.vf) return $x.id").execute(context)));
        }
      }
    }
  }

  @Test
  void comparisonPointsPreserveEmptyAndMultipleItemSemantics() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final String rows = "for $x in jn:doc('slice','" + resource + "')[] where ";
        context.bind(new QNm("p"), new ItemSequence());
        assertEquals(List.of(), values(new Query(chain, "declare variable $p external; " + rows
            + "xs:dateTime($x.vf) le $p and $p lt xs:dateTime($x.vt) return $x.id").execute(context)));
        context.bind(new QNm("p"), new ItemSequence(new DateTime("2024-01-01T00:00:00Z"),
            new DateTime("2024-01-01T00:00:00Z")));
        final String points = "declare variable $p external; ";
        assertEquals(List.of(1L), values(new Query(chain, points + rows
            + "xs:dateTime($x.vf) <= $p and $p < xs:dateTime($x.vt) return $x.id").execute(context)));
        assertThrows(QueryException.class, () -> values(new Query(chain, points + rows
            + "xs:dateTime($x.vf) le $p and $p lt xs:dateTime($x.vt) return $x.id").execute(context)));
      }
    }
  }

  @Test
  void emptyArraysDoNotEvaluateThePointCardinality() {
    create("[]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        context.bind(new QNm("p"), new ItemSequence(new DateTime("2024-01-01T00:00:00Z"),
            new DateTime("2024-01-01T00:00:00Z")));
        assertEquals(List.of(), values(new Query(chain, "declare variable $p external; "
            + "for $x in jn:doc('slice','" + resource
            + "')[] where xs:dateTime($x.vf) le $p and $p lt xs:dateTime($x.vt) return $x.id").execute(context)));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"vf", "vt"})
  void residualErrorsWaitForTheRequestedCandidate(final String field) {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":2,"vf":"%s","vt":"%s"}]
        """.formatted(field.equals("vf") ? "bad" : "2023-01-01T00:00:00Z",
            field.equals("vt") ? "bad" : "2025-01-01T00:00:00Z"));
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final String predicate = field.equals("vf") ? "xs:dateTime($x.vf) lt " + POINT
            : POINT + " lt xs:dateTime($x.vt)";
        final String expression = "for $x in " + source(resource) + " where " + predicate + " return $x";
        final Sequence sequence = new Query(chain, expression).execute(context);
        try (final Iter iterator = sequence.iterate()) {
          assertEquals(1, ((Numeric) ((JsonDBObject) iterator.next()).get(new QNm("id"))).intValue());
          assertThrows(QueryException.class, iterator::next);
        }
        final Sequence first = new Query(chain, "subsequence((" + expression + "),1,1)").execute(context);
        try (final Iter iterator = first.iterate()) {
          assertEquals(1, ((Numeric) ((JsonDBObject) iterator.next()).get(new QNm("id"))).intValue());
        }
      }
      final JsonDBItem document = store.lookup("slice").getDocument("indexed");
      final Sequence sequence = ValidTimeIntervalIndex.sequence(document, Instant.parse("2024-01-01T00:00:00Z"),
          document.getResourceSession().getResourceConfig().getValidTimeConfig(), field.equals("vf"), !field.equals("vf"),
          new ValidTimeResidual(null, context, () -> new DateTime("2024-01-01T00:00:00Z"), field,
              field.equals("vf"), true, false, field.equals("vf")));
      assertEquals(1, ((Numeric) ((JsonDBObject) sequence.get(Int32.ONE)).get(new QNm("id"))).intValue());
      assertThrows(QueryException.class, () -> sequence.get(new Int32(2)));
      assertThrows(QueryException.class, sequence::size);
    }
  }

  @ParameterizedTest
  @CsvSource({"vf,build", "vt,build", "vf,update", "vt,update", "vf,seed", "vt,seed", "vf,insert", "vt,insert"})
  void emptyFractionsRetainCastErrorsAcrossBuildUpdateSeedAndInsert(final String field, final String route) {
    final String raw = field.equals("vf") ? "2023-01-01T00:00:00.Z" : "2025-01-01T00:00:00.Z";
    final String good = "{\"id\":1,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}";
    final String bad = "{\"id\":2,\"vf\":\"%s\",\"vt\":\"%s\"}".formatted(
        field.equals("vf") ? raw : "2023-01-01T00:00:00Z",
        field.equals("vt") ? raw : "2025-01-01T00:00:00Z");
    create("[" + good + (route.equals("insert") ? "" : "," +
        (route.equals("update") ? good.replace("\"id\":1", "\"id\":2") : bad)) + "]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      if (!route.equals("build")) {
        for (final String resource : List.of("indexed", "plain")) {
          final JsonDBItem document = store.lookup("slice").getDocument(resource);
          final var session = document.getResourceSession();
          final JsonNodeTrx writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
          assertTrue(writer.moveTo(document.getNodeKey()));
          if (route.equals("insert")) {
            writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(bad), JsonNodeTrx.Commit.NO);
          } else {
            assertTrue(writer.moveToFirstChild());
            assertTrue(writer.moveToRightSibling());
            assertTrue(writer.moveToFirstChild());
            final String changedField = route.equals("seed") ? (field.equals("vf") ? "vt" : "vf") : field;
            while (!changedField.equals(writer.getName().getLocalName())) {
              assertTrue(writer.moveToRightSibling());
            }
            writer.setStringValue(route.equals("seed")
                ? (field.equals("vf") ? "2026-01-01T00:00:00Z" : "2022-01-01T00:00:00Z") : raw);
          }
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final QNm castError = assertThrows(QueryException.class, () -> new DateTime(raw)).getCode();
      final String comparison = field.equals("vf") ? "xs:dateTime($x.vf) lt " + POINT
          : POINT + " lt xs:dateTime($x.vt)";
      assertEquals(castError, assertThrows(QueryException.class, () -> new Query(chain,
          "count(for $x in " + source("indexed") + " where " + comparison + " return $x)")
          .evaluate(context)).getCode());
      final JsonDBItem document = store.lookup("slice").getDocument("indexed");
      final var cursor = document.getTrx();
      assertTrue(cursor.moveTo(document.getNodeKey()));
      assertTrue(cursor.moveToFirstChild());
      assertTrue(cursor.moveToRightSibling());
      final long badKey = cursor.getNodeKey();
      final IndexDef definition = document.getResourceSession().getRtxIndexController(cursor.getRevisionNumber())
          .getIndexes().getIndexDefs().stream().filter(IndexDef::isValidTimeIndex).findFirst().orElseThrow();
      final LongOpenHashSet unverified = new LongOpenHashSet();
      ValidTimeIntervalIndexFactory.createVerificationStore(cursor.getStorageEngineReader(), definition.getID())
          .scan(0, 0, 0, unverified::add);
      assertEquals(LongOpenHashSet.of(badKey), unverified);
      final JsonDBObject badObject = new JsonDBObject(cursor, document.getCollection());
      final Sequence badOnly = ValidTimeIntervalIndex.sequence(badObject, Instant.parse("2024-01-01T00:00:00Z"),
          document.getResourceSession().getResourceConfig().getValidTimeConfig(), field.equals("vf"), field.equals("vt"),
          new ValidTimeResidual(null, context, () -> new DateTime("2024-01-01T00:00:00Z"), field,
              field.equals("vf"), true, false, field.equals("vf")));
      assertEquals(castError, assertThrows(QueryException.class, badOnly::booleanValue).getCode());
      for (final String resource : List.of("indexed", "plain")) {
        assertEquals(2, ((Numeric) new Query(chain, "count(" + source(resource) + ")").evaluate(context)).intValue());
        final String other = field.equals("vf") ? POINT + " lt xs:dateTime($x.vt)" : "xs:dateTime($x.vf) lt " + POINT;
        assertEquals(List.of(1L, 2L), values(new Query(chain,
            "for $x in " + source(resource) + " where " + other + " return $x.id").execute(context)));
        for (final String operator : List.of("lt", "le", "<", "<=")) {
          for (final boolean mirror : new boolean[] {false, true}) {
            final String inverse = switch (operator) {
              case "lt" -> "gt";
              case "le" -> "ge";
              case "<" -> ">";
              default -> ">=";
            };
            final String bound = "xs:dateTime($x." + field + ")";
            final String predicate = mirror
                ? (field.equals("vf") ? POINT + " " + inverse + " " + bound : bound + " " + inverse + " " + POINT)
                : (field.equals("vf") ? bound + " " + operator + " " + POINT : POINT + " " + operator + " " + bound);
            final String folded = "for $x in " + source(resource) + " where " + predicate + " return $x";
            final String plainSource = "for $x in jn:doc('slice','" + resource + "')[] where ";
            for (final String expression : List.of(folded, plainSource + predicate + " and " + other + " return $x",
                plainSource + other + " and " + predicate + " return $x")) {
              final Sequence rows = new Query(chain, expression).execute(context);
              assertEquals(1, ((Numeric) ((JsonDBObject) rows.get(Int32.ONE)).get(new QNm("id"))).intValue());
              assertEquals(castError, assertThrows(QueryException.class, () -> rows.get(new Int32(2))).getCode());
              assertEquals(castError, assertThrows(QueryException.class, rows::size).getCode());
              assertEquals(castError, assertThrows(QueryException.class,
                  () -> new Query(chain, "count(" + expression + ")").evaluate(context)).getCode());
              try (final Iter iterator = rows.iterate()) {
                assertEquals(1, ((Numeric) ((JsonDBObject) iterator.next()).get(new QNm("id"))).intValue());
                assertEquals(castError, assertThrows(QueryException.class, iterator::next).getCode());
              }
            }
          }
        }
        final String outside = field.equals("vf") ? "xs:dateTime('2019-01-01T00:00:00Z')"
            : "xs:dateTime('2030-01-01T00:00:00Z')";
        final String predicate = field.equals("vf") ? "xs:dateTime($x.vf) le " + outside
            : outside + " lt xs:dateTime($x.vt)";
        assertEquals(0, ((Numeric) new Query(chain, "count(for $x in " + source(resource).replace(POINT, outside)
            + " where " + predicate + " return $x)").evaluate(context)).intValue());
        assertEquals(castError, assertThrows(QueryException.class, () -> new Query(chain,
            "count(for $x in jn:doc('slice','" + resource + "')[] where " + predicate + " and "
                + other.replace(POINT, outside) + " return $x)").evaluate(context)).getCode());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"interval", "cas", "linear"})
  void foldedFallbackPreservesClosedSourceOrderAndFirstDemand(final String route) {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z",
          "otherFrom":"2023-01-01T00:00:00Z","otherTo":"2025-01-01T00:00:00Z"},
         {"id":2,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z",
          "otherFrom":"2023-01-01T00:00:00Z","otherTo":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      if (route.equals("interval")) {
        index(chain, context);
      }
      final JsonDBItem document = store.lookup("slice").getDocument("indexed");
      final var cursor = document.getTrx();
      cursor.moveTo(document.getNodeKey());
      assertTrue(cursor.moveToFirstChild());
      assertTrue(cursor.moveToRightSibling());
      final long second = cursor.getNodeKey();
      final var session = document.getResourceSession();
      final JsonNodeTrx writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
      if (route.equals("cas")) {
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(
            IndexDefs.createCASIdxDef(false, Type.DATI, Set.of(parse("/[]/vf", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON),
            IndexDefs.createCASIdxDef(false, Type.DATI, Set.of(parse("/[]/vt", PathParser.Type.JSON)), 1, IndexDef.DbType.JSON)), writer);
      }
      writer.moveTo(document.getNodeKey());
      writer.moveSubtreeToFirstChild(second);
      writer.commit();
      final List<Long> expected = route.equals("linear") ? List.of(2L, 1L) : List.of(1L, 2L);
      for (final String point : List.of(POINT, "xs:dateTime('2024-01-01T00:00:00')")) {
        final String source = source("indexed").replace(POINT, point);
        assertEquals(expected, values(new Query(chain, "for $x in " + source + " return $x.id").execute(context)));
        for (final boolean start : List.of(false, true)) {
          for (final String field : start ? List.of("vf", "otherFrom") : List.of("vt", "otherTo")) {
            for (final boolean strict : List.of(false, true)) {
              for (final boolean general : List.of(false, true)) {
                for (final boolean mirror : List.of(false, true)) {
                  final String predicate = comparison(field, start, strict, general, mirror, point);
                  final String rows = "for $x in " + source + " where " + predicate + " return $x";
                  final String reference = "for $x at $position in " + source + " where " + predicate + " return $x.id";
                  assertEquals(expected, values(new Query(chain, reference).execute(context)), reference);
                  assertEquals(expected, values(new Query(chain, rows + ".id").execute(context)), rows);
                  final Sequence first = new Query(chain, "subsequence((" + rows + "),1,1)").execute(context);
                  try (final Iter iterator = first.iterate()) {
                    assertEquals(expected.getFirst().longValue(),
                        ((Numeric) ((JsonDBObject) iterator.next()).get(new QNm("id"))).longValue(), rows);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  void bitemporalFoldingPreservesUntypedComparisonOperandAndKind() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      context.bind(new QNm("p"), new Una("2024-01-01T00:00:00Z"));
      for (final String resource : List.of("indexed", "plain")) {
        final String source = source(resource).replace(POINT, "$p");
        for (final boolean start : List.of(false, true)) {
          for (final boolean strict : List.of(false, true)) {
            for (final boolean general : List.of(false, true)) {
              for (final boolean mirror : List.of(false, true)) {
                final String predicate = comparison(start ? "vf" : "vt", start, strict, general, mirror, "$p");
                final String prefix = "declare variable $p external; for $x";
                final String suffix = " in " + source + " where " + predicate + " return $x.id";
                final Query reference = new Query(chain, prefix + " at $position" + suffix);
                final Query folded = new Query(chain, prefix + suffix);
                if (general) {
                  assertEquals(List.of(1L), values(reference.execute(context)), suffix);
                  assertEquals(List.of(1L), values(folded.execute(context)), suffix);
                } else {
                  final QueryException expected = assertThrows(QueryException.class,
                      () -> values(reference.execute(context)), suffix);
                  final QueryException actual = assertThrows(QueryException.class,
                      () -> values(folded.execute(context)), suffix);
                  assertEquals(expected.getCode(), actual.getCode(), suffix);
                }
              }
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"[1]", "[null]", "[true]", "[\"text\"]", "[[]]", "[]"})
  void plainFallbackPreservesNonobjectAndEmptyArrayEvaluation(final String json) {
    create(json);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      context.bind(new QNm("p"), new ItemSequence(new DateTime("2024-01-01T00:00:00Z"),
          new DateTime("2024-01-01T00:00:00Z")));
      for (final boolean firstStart : List.of(false, true)) {
        for (final boolean firstGeneral : List.of(false, true)) {
          for (final boolean secondGeneral : List.of(false, true)) {
            for (final boolean mirror : List.of(false, true)) {
              final String first = comparison(firstStart ? "vf" : "vt", firstStart, !firstStart,
                  firstGeneral, mirror, "$p");
              final String second = comparison(firstStart ? "vt" : "vf", !firstStart, firstStart,
                  secondGeneral, mirror, "$p");
              final String prefix = "declare variable $p external; for $x in jn:doc('slice','";
              final String suffix = "')[] where " + first + " and " + second + " return 1";
              final Query reference = new Query(chain, prefix + "plain" + suffix);
              final Query folded = new Query(chain, prefix + "indexed" + suffix);
              if (json.equals("[]") || firstGeneral) {
                assertEquals(List.of(), values(reference.execute(context)), suffix);
                assertEquals(List.of(), values(folded.execute(context)), suffix);
              } else {
                final QueryException expected = assertThrows(QueryException.class,
                    () -> values(reference.execute(context)), suffix);
                final QueryException actual = assertThrows(QueryException.class,
                    () -> values(folded.execute(context)), suffix);
                assertEquals(expected.getCode(), actual.getCode(), suffix);
              }
            }
          }
        }
      }
    }
  }

  private static String comparison(final String field, final boolean start, final boolean strict,
      final boolean general, final boolean mirror, final String point) {
    final String operator = general ? (strict ? "<" : "<=") : (strict ? "lt" : "le");
    final String bound = "xs:dateTime($x." + field + ")";
    if (!mirror) {
      return start ? bound + " " + operator + " " + point : point + " " + operator + " " + bound;
    }
    final String swapped = general ? (strict ? ">" : ">=") : (strict ? "gt" : "ge");
    return start ? point + " " + swapped + " " + bound : bound + " " + swapped + " " + point;
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "[1]",
      "[{\"id\":1,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}]",
      "[{\"id\":1,\"vf\":\"field-error\",\"vt\":\"field-error\"}]"})
  void pointExpressionsWaitForRowsAndPreserveComparisonErrorOrder(final String json) {
    create(json);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      final String point = "xs:dateTime('point-error')";
      for (int shape = 0; shape < 128; shape++) {
        final String start = comparison("vf", true, (shape & 1) != 0, (shape & 4) != 0,
            (shape & 16) != 0, point);
        final String end = comparison("vt", false, (shape & 2) != 0, (shape & 8) != 0,
            (shape & 32) != 0, point);
        final String predicate = (shape & 64) == 0 ? start + " and " + end : end + " and " + start;
        final String suffix = "')[] where " + predicate + " return $x";
        final Query reference = new Query(chain, "for $x in jn:doc('slice','plain" + suffix);
        final Query folded = new Query(chain, "for $x in jn:doc('slice','indexed" + suffix);
        final Sequence expectedRows = assertDoesNotThrow(() -> reference.execute(context), predicate);
        final Sequence actualRows = assertDoesNotThrow(() -> folded.execute(context), predicate);
        try (Iter iterator = actualRows.iterate()) {
          assertNotNull(iterator);
        }
        assertNull(actualRows.get(Int32.ZERO), predicate);
        if (json.equals("[]")) {
          assertEquals(0, expectedRows == null ? 0 : expectedRows.size().intValue(), predicate);
          assertEquals(0, actualRows.size().intValue(), predicate);
          assertFalse(actualRows.booleanValue(), predicate);
          assertNull(actualRows.get(Int32.ONE), predicate);
        } else {
          final QueryException expected = assertThrows(QueryException.class, expectedRows::size, predicate);
          final QueryException actual = assertThrows(QueryException.class, actualRows::size, predicate);
          assertEquals(expected.getCode(), actual.getCode(), predicate);
          assertEquals(expected.getMessage(), actual.getMessage(), predicate);
        }
      }
    }
  }

  @Test
  void deferredPointsRetainTheirOuterTupleAcrossRepeatedIteration() {
    create("""
        [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":2,"vf":"2025-01-01T00:00:00Z","vt":"2027-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      index(chain, context);
      for (final String resource : List.of("indexed", "plain")) {
        final String text = "for $p in (" + POINT + ",xs:dateTime('2026-01-01T00:00:00Z')) "
            + "for $x in jn:doc('slice','" + resource + "')[] where xs:dateTime($x.vf) le $p "
            + "and $p lt xs:dateTime($x.vt) return $x.id";
        final Sequence rows = new Query(chain, text).execute(context);
        assertEquals(List.of(1L, 2L), values(rows));
        assertEquals(List.of(1L, 2L), values(rows));
      }
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
