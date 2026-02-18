package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for parallel (block-based) query execution using Brackit's
 * {@link io.brackit.query.compiler.translator.BlockPipelineStrategy}.
 *
 * <p>Verifies that per-worker read-only transactions (via ThreadSafeJsonReadOnlyTrx)
 * correctly support concurrent access from ForkJoinPool workers.
 */
public final class ParallelQueryExecutionTest {

  private final Path sirixPath = PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  void testSimpleParallelForQuery() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store)) {

      // Store JSON array
      new Query(seqChain, "jn:store('json-path1','mydoc.jn','[1, 2, 3, 4, 5]')").evaluate(ctx);

      // Simple parallel for-each that reads array elements
      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          for $item in $doc[]
          return $item
          """;
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }
      assertEquals("1 2 3 4 5", buf.toString());
    }
  }

  @Test
  void testParallelForQueryWithFilter() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store)) {

      // Store JSON array of objects
      new Query(seqChain,
          "jn:store('json-path1','mydoc.jn','[{\"name\":\"a\",\"val\":1},{\"name\":\"b\",\"val\":2},{\"name\":\"c\",\"val\":3}]')")
          .evaluate(ctx);

      // Parallel for with where clause
      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          for $item in $doc[]
          where $item.val > 1
          return $item.name
          """;
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }
      assertEquals("\"b\" \"c\"", buf.toString());
    }
  }

  @Test
  void testParallelForQueryWithLetBinding() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store)) {

      new Query(seqChain,
          "jn:store('json-path1','mydoc.jn','[{\"x\":10},{\"x\":20},{\"x\":30}]')")
          .evaluate(ctx);

      // Parallel for with let binding
      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          for $item in $doc[]
          let $doubled := $item.x * 2
          return $doubled
          """;
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }
      assertEquals("20 40 60", buf.toString());
    }
  }

  @Test
  void testParallelForQueryWithDeref() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store)) {

      // Store nested JSON
      new Query(seqChain,
          "jn:store('json-path1','mydoc.jn','{\"items\":[{\"id\":1,\"label\":\"foo\"},{\"id\":2,\"label\":\"bar\"},{\"id\":3,\"label\":\"baz\"}]}')")
          .evaluate(ctx);

      // Parallel for iterating nested array with deref
      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          for $item in $doc.items[]
          return $item.label
          """;
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }
      assertEquals("\"foo\" \"bar\" \"baz\"", buf.toString());
    }
  }

  @Test
  void testParallelSimpleDerefQuery() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store)) {

      new Query(seqChain,
          "jn:store('json-path1','mydoc.jn','{\"hello\":\"world\"}')")
          .evaluate(ctx);

      // Simple deref (no FLWOR, no parallelism triggered but proxy still used)
      final var query = "jn:doc('json-path1','mydoc.jn').hello";
      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }
      assertEquals("\"world\"", buf.toString());
    }
  }

  @Test
  void testParallelMatchesSequentialForReadOnlyQuery() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store, true)) {

      new Query(seqChain,
          "jn:store('json-path1','mydoc.jn','[{\"a\":1},{\"a\":2},{\"a\":3},{\"a\":4},{\"a\":5}]')")
          .evaluate(ctx);

      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          for $item in $doc[]
          return $item.a
          """;

      // Execute sequentially
      final var seqBuf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(seqBuf)) {
        serializer.serialize(new Query(seqChain, query).execute(ctx));
      }

      // Execute in parallel (ordered)
      final var parBuf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(parBuf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }

      assertEquals(seqBuf.toString(), parBuf.toString());
    }
  }

  @Test
  void testParallelLargerDataset() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store, true)) {

      // Generate a larger JSON array to exercise work-stealing
      final var sb = new StringBuilder("[");
      for (int i = 0; i < 100; i++) {
        if (i > 0) sb.append(',');
        sb.append("{\"id\":").append(i).append(",\"value\":\"item").append(i).append("\"}");
      }
      sb.append(']');
      new Query(seqChain, "jn:store('json-path1','mydoc.jn','" + sb + "')").evaluate(ctx);

      // Count items via FLWOR
      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          return count(for $item in $doc[] return $item)
          """;

      final var seqBuf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(seqBuf)) {
        serializer.serialize(new Query(seqChain, query).execute(ctx));
      }

      final var parBuf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(parBuf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }

      assertEquals("100", seqBuf.toString());
      assertEquals("100", parBuf.toString());
    }
  }

  @Test
  void testParallelOrderByQuery() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var seqChain = SirixCompileChain.createWithJsonStore(store);
        final var parChain = SirixCompileChain.createParallel(null, store, true)) {

      new Query(seqChain,
          "jn:store('json-path1','mydoc.jn','[{\"v\":3},{\"v\":1},{\"v\":2}]')")
          .evaluate(ctx);

      final var query = """
          let $doc := jn:doc('json-path1','mydoc.jn')
          for $item in $doc[]
          order by $item.v
          return $item.v
          """;

      final var seqBuf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(seqBuf)) {
        serializer.serialize(new Query(seqChain, query).execute(ctx));
      }

      final var parBuf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(parBuf)) {
        serializer.serialize(new Query(parChain, query).execute(ctx));
      }

      assertEquals(seqBuf.toString(), parBuf.toString());
    }
  }
}
