package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

final class ValidTimeEmptySourceTest {
  private static final String POINT = "xs:dateTime('2024-01-01T00:00:00Z')";
  private static final String OBJECT = "{\"id\":1,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}";

  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"", "{}", OBJECT, "[]", "\"text\"", "7", "true", "null"})
  void unboxingEmptySourcesPreservesOriginalResultsAndNeverEvaluatesDeferredPoints(final String json) {
    create(json);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final var collection = store.lookup("empty");
      final JsonDBItem document = collection.getDocument("indexed");
      final var indexedSession = collection.getDatabase().beginResourceSession("indexed");
      assertTrue(indexedSession.getRtxIndexController(indexedSession.getMostRecentRevisionNumber())
                               .getIndexes()
                               .getIndexDefs()
                               .stream()
                               .anyMatch(IndexDef::isValidTimeIndex));
      for (final String point : new String[] {POINT, "xs:dateTime('point-error')"}) {
        for (final String resource : new String[] {"indexed", "plain"}) {
          final String predicate =
              "[] where xs:dateTime($x.vf) le " + point + " and " + point + " lt xs:dateTime($x.vt) return $x";
          final String source = "jn:doc('empty','" + resource + "')";
          assertEquals(0, count(chain, context, "for $x in " + source + predicate));
          assertEquals(0, count(generic, context, "for $x at $position in " + source + predicate));
        }
      }
      final AtomicInteger evaluations = new AtomicInteger();
      final Supplier<Sequence> point = () -> {
        evaluations.incrementAndGet();
        throw new QueryException(new QNm("point-error"));
      };
      final StaticContext staticContext = mock(StaticContext.class);
      for (int mode = 0; mode < 128; mode++) {
        assertEmpty(ScanValidTimeIndex.comparisonScan(staticContext, context, document, point, "vf", "vt", mode));
        assertEmpty(ValidTimeFilter.comparisonScanSequence(document, point, "vf", "vt", mode, staticContext, context));
      }
      assertEquals(0, evaluations.get());
      if (json.equals(OBJECT)) {
        assertEquals(1, count(chain, context, "jn:valid-at('empty','indexed'," + POINT + ")"));
        assertEquals(1, count(chain, context, "jn:scan-valid-time-index(jn:doc('empty','indexed')," + POINT + ")"));
        assertEquals(1, count(chain, context,
            "jn:open-bitemporal('empty','indexed',xs:dateTime('2099-01-01T00:00:00Z')," + POINT + ")"));
      } else if (json.isEmpty()) {
        assertNull(document);
        assertThrows(QueryException.class,
            () -> new Query(chain, "jn:valid-at('empty','indexed'," + POINT + ")").evaluate(context));
        assertThrows(QueryException.class,
            () -> new Query(chain,
                "jn:open-bitemporal('empty','indexed',xs:dateTime('2099-01-01T00:00:00Z')," + POINT + ")").evaluate(
                    context));
      }
    }
  }

  @Test
  void comparisonBoundarySkipsConfigurationForNonArraysAndTheComparisonOverload() {
    final JsonDBItem source = mock(JsonDBItem.class);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store)) {
      final Sequence[] args = {source, null, new Str("vf"), new Str("vt"), Int32.ZERO};
      assertEmpty(ScanValidTimeIndex.forComparisons().execute(mock(StaticContext.class), context, args));
      verify(source, never()).getTrx();
      verify(source, never()).getResourceSession();
    }
  }

  @Test
  void aCompiledComparisonResolvesObjectArrayAndEmptyHistoricalRevisionsAtEvaluation() {
    create(OBJECT);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final var collection = store.lookup("empty");
      final JsonDBItem object = collection.getDocument("indexed");
      final var session = object.getResourceSession();
      final int objectRevision = session.getMostRecentRevisionNumber();
      final JsonNodeTrx writer = session.beginNodeTrx();
      writer.moveTo(object.getNodeKey());
      writer.remove();
      writer.moveToDocumentRoot();
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + OBJECT + "]"), JsonNodeTrx.Commit.NO);
      writer.commit();
      final int arrayRevision = session.getMostRecentRevisionNumber();
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.remove();
      writer.commit();
      final int emptyRevision = session.getMostRecentRevisionNumber();
      final String source = "jn:doc('empty','indexed',$revision)";
      final String predicate =
          "[] where xs:dateTime($x.vf) le " + POINT + " and " + POINT + " lt xs:dateTime($x.vt) return $x";
      final Query optimized =
          new Query(chain, "declare variable $revision external; count(for $x in " + source + predicate + ")");
      final Query original = new Query(generic,
          "declare variable $revision external; count(for $x at $position in " + source + predicate + ")");
      for (final int revision : new int[] {objectRevision, arrayRevision, emptyRevision, arrayRevision,
          objectRevision}) {
        context.bind(new QNm("revision"), new Int32(revision));
        final int expected = revision == arrayRevision
            ? 1
            : 0;
        assertEquals(expected, ((Numeric) optimized.evaluate(context)).intValue());
        assertEquals(expected, ((Numeric) original.evaluate(context)).intValue());
      }
      assertNotNull(collection.getDocument("indexed", objectRevision));
      assertNull(collection.getDocument("indexed", emptyRevision));
    }
  }

  private void create(final String json) {
    final Path databasePath = directory.resolve("empty");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      for (final String resource : new String[] {"indexed", "plain"}) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .validTimePaths("vf", "vt")
                                                     .build());
        try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
          switch (json) {
            case "" -> {
            }
            case "\"text\"" -> writer.insertStringValueAsFirstChild("text");
            case "7" -> writer.insertNumberValueAsFirstChild(7);
            case "true" -> writer.insertBooleanValueAsFirstChild(true);
            case "null" -> writer.insertNullValueAsFirstChild();
            default -> writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
          }
          if (resource.equals("indexed")) {
            session.getWtxIndexController(writer.getRevisionNumber())
                   .createIndexes(Set.of(IndexDefs.createValidTimeIdxDef(
                       Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), 0,
                       IndexDef.DbType.JSON)), writer);
          }
          writer.commit();
        }
      }
    }
  }

  private static int count(final SirixCompileChain chain, final SirixQueryContext context, final String rows) {
    return ((Numeric) new Query(chain, "count(" + rows + ")").evaluate(context)).intValue();
  }

  private static void assertEmpty(final Sequence sequence) {
    assertEquals(0, sequence.size().intValue());
    assertFalse(sequence.booleanValue());
    assertNull(sequence.get(Int32.ONE));
    try (var iterator = sequence.iterate()) {
      assertNull(iterator.next());
    }
  }
}
