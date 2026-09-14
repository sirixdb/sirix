/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.bench.clickbench.ClickBenchSource;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.ProjectionSpec;
import io.sirix.query.scan.SirixVectorizedExecutor;
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
    final ProjectionSpec spec = new ProjectionSpec("/[]",
        List.of("/[]/kind", "/[]/did", "/[]/time_us", "/[]/commit/collection",
            "/[]/commit/operation"),
        List.of("string", "string", "long", "string", "string"),
        new ProjectionSortedSpec(List.of(1, 2),
            List.of(new ProjectionSortedSpec.Equality(0, "commit"),
                new ProjectionSortedSpec.Equality(4, "create"),
                new ProjectionSortedSpec.Equality(3, "post"))));
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
    final ProjectionSpec spec = new ProjectionSpec("/[]",
        List.of("/[]/kind", "/[]/did", "/[]/time_us", "/[]/commit/collection",
            "/[]/commit/operation"),
        List.of("string", "string", "long", "string", "string"),
        new ProjectionSortedSpec(List.of(1, 2),
            List.of(new ProjectionSortedSpec.Equality(0, "commit"),
                new ProjectionSortedSpec.Equality(4, "create"),
                new ProjectionSortedSpec.Equality(3, "post"))));
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).hashType(HashType.NONE)
        .storeNodeHistory(true).build();
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
    final ProjectionSpec spec = new ProjectionSpec("/[]",
        List.of("/[]/tenant", "/[]/sku", "/[]/price", "/[]/state"),
        List.of("string", "string", "long", "string"),
        new ProjectionSortedSpec(List.of(1, 2),
            List.of(new ProjectionSortedSpec.Equality(3, "active"),
                new ProjectionSortedSpec.Equality(0, "north"))));
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("inventory", "entries",
          new JsonReader(new StringReader(INVENTORY_ROWS)), spec);
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

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext context,
      final String query) throws IOException {
    final StringWriter buffer = new StringWriter();
    try (PrintWriter output = new PrintWriter(buffer)) {
      new Query(chain, query).serialize(context, output);
    }
    return buffer.toString();
  }
}
