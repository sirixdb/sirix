/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.trx.node.HashType;
import io.brackit.query.QueryException;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.bench.clickbench.ClickBenchSource;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.ProjectionSpec;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedGroupServingTest {

  @TempDir
  Path directory;

  private static final String ROWS = """
      [{"kind":"commit","did":"a","time_us":1000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"b","time_us":2000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"c","time_us":3000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"d","time_us":4000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"a","time_us":5000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"b","time_us":9000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"c","time_us":4000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"d","time_us":12000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"identity","did":"ignored","time_us":1}]
      """;

  /** Rows outside the filter prefix sort before and after it; one row's user id is not a string. */
  private static final String ROWS_WITH_UNENCODABLE_KEY = """
      [{"kind":"commit","did":"a","time_us":1000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"b","time_us":2000,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":7,"time_us":500,"commit":{"operation":"create","collection":"post"}},
       {"kind":"commit","did":"c","time_us":3000,"commit":{"operation":"create","collection":"like"}},
       {"kind":"identity","did":"ignored","time_us":1}]
      """;

  private static final List<String> EVENT_FIELDS =
      List.of("/[]/kind", "/[]/did", "/[]/time_us", "/[]/commit/collection", "/[]/commit/operation");

  private static final List<String> EVENT_TYPES = List.of("string", "string", "long", "string", "string");

  /** kind, operation, collection, did, time_us — columns only, no filter literal. */
  private static final ProjectionSortedSpec EVENT_ORDER = new ProjectionSortedSpec(List.of(0, 4, 3, 1, 2));

  private static final String NDJSON_ROWS = """
      {"kind":"commit","did":"a","time_us":1000,"commit":{"operation":"create","collection":"post"}}
      {"kind":"commit","did":"b","time_us":2000,"commit":{"operation":"create","collection":"post"}}
      {"kind":"commit","did":"a","time_us":5000,"commit":{"operation":"create","collection":"post"}}
      {"kind":"commit","did":"b","time_us":9000,"commit":{"operation":"create","collection":"post"}}
      {"kind":"identity","did":"ignored","time_us":1}
      """;

  private static final String FILTER = """
      $e.kind = "commit" and $e.commit.operation = "create" and $e.commit.collection = "post"
      """;

  private static final String EARLIEST = """
      subsequence(
        for $e in jn:doc('sorted','events')[]
        where %s
        let $k := $e.did
        group by $k
        let $first := min($e.time_us)
        order by $first
        return {"user_id": $k, "first": $first}, 1, 2)
      """.formatted(FILTER);

  private static final String SPAN = """
      subsequence(
        for $e in jn:doc('sorted','events')[]
        where %s
        let $k := $e.did
        group by $k
        let $first := min($e.time_us)
        let $last := max($e.time_us)
        let $span := ($last idiv 1000) - ($first idiv 1000)
        order by $span descending
        return {"user_id": $k, "span": $span}, 1, 2)
      """.formatted(FILTER);

  private static final String INVENTORY_ROWS = """
      [{"tenant":"north","sku":"widget","price":9,"state":"active"},
       {"tenant":"north","sku":"gadget","price":7,"state":"active"},
       {"tenant":"north","sku":"widget","price":3,"state":"active"},
       {"tenant":"north","sku":"gadget","price":11,"state":"active"},
       {"tenant":"north","sku":"widget","price":1,"state":"retired"},
       {"tenant":"south","sku":"gadget","price":2,"state":"active"}]
      """;

  private static final String INVENTORY_MIN = """
      subsequence(
        for $item in jn:doc('inventory','entries')[]
        where $item.tenant = "north" and $item.state = "active"
        let $sku := $item.sku
        group by $sku
        let $lowest := min($item.price)
        order by $lowest
        return {"sku": $sku, "lowest": $lowest}, 1, 2)
      """;

  @AfterEach
  void clearServing() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
  }

  @Test
  void groupedMinAndSpanUseExactRevisionedSortedView() throws IOException {
    final ProjectionSpec spec = new ProjectionSpec("/[]", EVENT_FIELDS, EVENT_TYPES, EVENT_ORDER);
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection =
          store.create("sorted", "events", new JsonReader(new StringReader(ROWS)), spec);
      final String expectedEarliest = evaluate(chain, context, EARLIEST);
      final String expectedSpan = evaluate(chain, context, SPAN);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("events")) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long before = SirixVectorizedExecutor.groupSortedServedCount();
          assertEquals(expectedEarliest, evaluate(chain, context, EARLIEST));
          assertEquals(expectedSpan, evaluate(chain, context, SPAN));
          assertEquals(2L, SirixVectorizedExecutor.groupSortedServedCount() - before);
          assertTrue(expectedEarliest.contains("\"user_id\":\"a\""));
          assertTrue(expectedSpan.contains("\"user_id\":\"d\""));
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"jackson", "parallel"})
  void streamingLoadersPublishSortedViewFromNdjson(final String loader) throws IOException {
    final Path source = Files.writeString(directory.resolve("events.ndjson"), NDJSON_ROWS);
    final ProjectionSpec spec = new ProjectionSpec("/[]", EVENT_FIELDS, EVENT_TYPES, EVENT_ORDER);
    try (
        BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(directory).hashType(HashType.NONE).storeNodeHistory(true).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection;
      if (loader.equals("jackson")) {
        try (ClickBenchSource.JacksonSource input = ClickBenchSource.openJackson(source.toString())) {
          collection = store.create("sorted", "events", input.parser(), spec, input.ldjson());
        }
      } else {
        try (InputStream input = ClickBenchSource.openParallelInput(source.toString(), false)) {
          collection = store.createParallel("sorted", "events", input, spec);
        }
      }
      final String expectedEarliest = evaluate(chain, context, EARLIEST);
      final String expectedSpan = evaluate(chain, context, SPAN);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("events")) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long before = SirixVectorizedExecutor.groupSortedServedCount();
          assertEquals(expectedEarliest, evaluate(chain, context, EARLIEST));
          assertEquals(expectedSpan, evaluate(chain, context, SPAN));
          assertEquals(2L, SirixVectorizedExecutor.groupSortedServedCount() - before);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  @Test
  void groupedMinUsesSortedViewForUnrelatedSchemaAndFilter() throws IOException {
    final ProjectionSpec spec = new ProjectionSpec("/[]", List.of("/[]/tenant", "/[]/sku", "/[]/price", "/[]/state"),
        List.of("string", "string", "long", "string"), new ProjectionSortedSpec(List.of(3, 0, 1, 2)));
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection =
          store.create("inventory", "entries", new JsonReader(new StringReader(INVENTORY_ROWS)), spec);
      final String expected = evaluate(chain, context, INVENTORY_MIN);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("entries")) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long before = SirixVectorizedExecutor.groupSortedServedCount();
          assertEquals(expected, evaluate(chain, context, INVENTORY_MIN));
          assertEquals(1L, SirixVectorizedExecutor.groupSortedServedCount() - before);
          assertTrue(expected.contains("\"sku\":\"widget\""));
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  @Test
  void unencodableSortKeyLoadsCommitsAndQueriesExactlyThroughTheFallback() throws IOException {
    final ProjectionSpec spec = new ProjectionSpec("/[]", EVENT_FIELDS, EVENT_TYPES, EVENT_ORDER);
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection =
          store.create("sorted", "events", new JsonReader(new StringReader(ROWS_WITH_UNENCODABLE_KEY)), spec);
      final String expectedEarliest = evaluate(chain, context, EARLIEST);
      final String expectedSpan = evaluate(chain, context, SPAN);
      assertTrue(expectedEarliest.contains("\"user_id\":7"), expectedEarliest);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("events")) {
        final SirixVectorizedExecutor executor =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long before = SirixVectorizedExecutor.groupSortedServedCount();
          assertEquals(expectedEarliest, evaluate(chain, context, EARLIEST));
          assertEquals(expectedSpan, evaluate(chain, context, SPAN));
          assertEquals(0L, SirixVectorizedExecutor.groupSortedServedCount() - before);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  @Test
  void secondPassCreateProjectionIndexDeclaresTheSameSortedView() throws IOException {
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("sorted", "events", new JsonReader(new StringReader(ROWS)));
      final String expectedEarliest = evaluate(chain, context, EARLIEST);
      final String expectedSpan = evaluate(chain, context, SPAN);
      evaluate(chain, context, """
          let $doc := jn:doc('sorted','events')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/kind', '/[]/did', '/[]/time_us', '/[]/commit/collection', '/[]/commit/operation'),
              ('string', 'string', 'long', 'string', 'string'),
              ('/[]/kind', '/[]/commit/operation', '/[]/commit/collection', '/[]/did', '/[]/time_us'))
          return sdb:commit($doc)
          """);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("events")) {
        final int revision = session.getMostRecentRevisionNumber();
        assertTrue(session.getRtxIndexController(revision)
                          .getIndexes()
                          .getIndexDefs()
                          .stream()
                          .anyMatch(definition -> EVENT_ORDER.equals(definition.getProjectionSortedSpec())));
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long before = SirixVectorizedExecutor.groupSortedServedCount();
          assertEquals(expectedEarliest, evaluate(chain, context, EARLIEST));
          assertEquals(expectedSpan, evaluate(chain, context, SPAN));
          assertEquals(2L, SirixVectorizedExecutor.groupSortedServedCount() - before);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  @Test
  void unsortableSortColumnIsRejectedBeforeTheOpenWriteTransactionIsTouched() throws IOException {
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("sorted", "events", new JsonReader(
          new StringReader("[{\"kind\":\"commit\",\"score\":1.5},{\"kind\":\"identity\",\"score\":2.5}]")));
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("events");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        assertTrue(writer.moveToDocumentRoot());
        assertTrue(writer.moveToFirstChild());
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"kind\":\"uncommitted\",\"score\":3.5}"),
            JsonNodeTrx.Commit.NO);
        assertThrows(QueryException.class, () -> evaluate(chain, context, """
            let $doc := jn:doc('sorted','events')
            return jn:create-projection-index($doc, '/[]', ('/[]/kind', '/[]/score'), ('string', 'double'),
                ('/[]/score'))
            """));
        assertEquals(0,
            session.getWtxIndexController(writer.getRevisionNumber())
                   .getIndexes()
                   .getNrOfIndexDefsWithType(IndexType.PROJECTION),
            "a rejected declaration must not be catalogued in the caller's transaction");
        assertTrue(writer.moveToDocumentRoot());
        assertTrue(writer.moveToFirstChild());
        assertEquals(3, writer.getChildCount(), "the caller's uncommitted work must survive the rejection");
        writer.rollback();
      }
    }
  }

  @Test
  void createWithoutSortColumnsReusesTheSortedProjection() throws IOException {
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("sorted", "events", new JsonReader(new StringReader(ROWS)));
      final String fields = """
          ('/[]/kind', '/[]/did', '/[]/time_us', '/[]/commit/collection', '/[]/commit/operation'),
          ('string', 'string', 'long', 'string', 'string')""";
      evaluate(chain, context, """
          let $doc := jn:doc('sorted','events')
          let $stats := jn:create-projection-index($doc, '/[]', %s,
              ('/[]/kind', '/[]/commit/operation', '/[]/commit/collection', '/[]/did', '/[]/time_us'))
          return sdb:commit($doc)
          """.formatted(fields));
      evaluate(chain, context, """
          let $doc := jn:doc('sorted','events')
          let $stats := jn:create-projection-index($doc, '/[]', %s)
          return sdb:commit($doc)
          """.formatted(fields));
      final String found = evaluate(chain, context, """
          jn:find-projection-index(jn:doc('sorted','events'), '/[]',
              ('/[]/kind', '/[]/did', '/[]/time_us', '/[]/commit/collection', '/[]/commit/operation'))
          """);
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("events")) {
        final Indexes indexes = session.getRtxIndexController(session.getMostRecentRevisionNumber()).getIndexes();
        assertEquals(1, indexes.getNrOfIndexDefsWithType(IndexType.PROJECTION),
            "a create without sort columns must reuse the same-shape sorted projection");
        final IndexDef sorted =
            indexes.getIndexDefs().stream().filter(IndexDef::isProjectionIndex).findFirst().orElseThrow();
        assertEquals(EVENT_ORDER, sorted.getProjectionSortedSpec());
        assertEquals(Integer.toString(sorted.getID()), found.trim());
      }
    }
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws IOException {
    final StringWriter buffer = new StringWriter();
    try (PrintWriter output = new PrintWriter(buffer)) {
      new Query(chain, query).serialize(context, output);
    }
    return buffer.toString();
  }
}
