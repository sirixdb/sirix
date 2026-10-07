package io.sirix.query.budget;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalQueries;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBCollection;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Counts actual source-array item reads, including repeated executions of the same compiled query.
 */
@Isolated
final class LetMaterializationWorkBudgetTest {
  private static final String ENABLED = "sirix.optimizer.materializeLets";
  private static final String MARKER = "SIRIX_MATERIALIZE_LET";
  @TempDir
  Path directory;

  @Test
  void sh1Q3ScansOncePerEvaluationInsteadOfOncePerReference() {
    final String query = BitemporalQueries.all().get(2).text();
    final Capture optimized = run(query, 100, true, 2);
    final Capture baseline = run(query, 100, false, 2);
    assertEquals("{\"min_cost\":1,\"max_cost\":1,\"prices\":1}", optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(200, optimized.reads, "one scan per enclosing evaluation");
    assertEquals(600, baseline.reads, "disabled rule proves the scan counter observes all three references");
    assertEquals(1, optimized.markers, "the executable Q3 plan marks the rows binding");
    assertEquals(0, baseline.markers);
    assertEquals(1, materializedPipes(JsonParser.parseString(optimized.planDump)),
        "the generated JSON plan dump must expose the materialized Q3 pipeline");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 10000})
  void emptySingletonLargeAndPartiallyConsumedResultsMatchGenericPlan(final int rows) {
    final String query = "let $rows := (for $r in jn:doc('bt','contracts')[] return $r.cost)"
        + " return [exists($rows),count($rows),sum($rows)][]";
    final Capture optimized = run(query, rows, true, 2);
    final Capture baseline = run(query, rows, false, 2);
    assertEquals((rows > 0 ? "true " : "false ") + rows + " " + (long) rows * (rows + 1) / 2, optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(2 * rows, optimized.reads);
    assertEquals(1, optimized.markers);
  }

  static Stream<Arguments> existenceOnlyConsumers() {
    return Stream.of(Arguments.of("[exists($rows),exists($rows)][]", "true true", 2),
        Arguments.of("[empty($rows),empty($rows)][]", "false false", 2),
        Arguments.of("[exists($rows),empty($rows)][]", "true false", 2),
        Arguments.of("{\"first\":exists($rows),\"second\":exists($rows)}", "{\"first\":true,\"second\":true}", 2),
        Arguments.of("(exists($rows),exists($rows))", "true true", 2),
        Arguments.of("[exists(data($rows)),exists(data($rows))][]", "true true", 2),
        Arguments.of("[exists($rows),if (exists($rows)) then exists($rows) else sum($rows)][]", "true true", 3),
        Arguments.of("[exists($rows),exists($rows) or sum($rows) gt 0][]", "true true", 2),
        Arguments.of("[exists($rows),empty($rows) and count($rows) gt 0][]", "true false", 2),
        Arguments.of("[exists($rows),exists($rows),()[sum($rows)]][]", "true true", 2),
        Arguments.of("[exists($rows),exists((1,sum($rows))),exists((1,count($rows)))][]", "true true true", 1));
  }

  @ParameterizedTest
  @MethodSource("existenceOnlyConsumers")
  void existenceOnlyConsumersKeepConstantSourceWork(final String result, final String answer, final int reads) {
    final String query = "let $rows := (for $r in jn:doc('bt','contracts')[] return $r.cost) return " + result;
    final Capture optimized = run(query, 10000, true, 2);
    final Capture baseline = run(query, 10000, false, 2);
    assertEquals(answer, optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(2L * reads, baseline.reads, "the disabled plan observes first-item demand on every evaluation");
    assertTrue(optimized.reads > 0, "the source-read counter must observe item demand");
    assertTrue(optimized.reads <= 2L * reads, "existence checks must not scan or buffer the 10,000-row source");
    assertEquals(0, optimized.markers);
  }

  @Test
  void correlatedBindingMaterializesAgainForEveryOuterTuple() {
    final String query =
        "for $n in 1 to 3" + " let $rows := (for $r in jn:doc('bt','contracts')[] where $r.cost le $n return $r.cost)"
            + " return [count($rows),sum($rows)][]";
    final Capture optimized = run(query, 10, true, 2);
    final Capture baseline = run(query, 10, false, 2);
    assertEquals("1 1 2 3 3 6", optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(60, optimized.reads, "three independent bindings per query evaluation");
  }

  @Test
  void nestedBindingsMatchGenericPlan() {
    final String query =
        "let $rows := (for $r in jn:doc('bt','contracts')[]" + " let $inner := (for $n in 1 to 2 return $r.cost * $n)"
            + " return [sum($inner),count($inner)][]) return [sum($rows),count($rows)][]";
    final Capture optimized = run(query, 10, true, 2);
    final Capture baseline = run(query, 10, false, 2);
    assertEquals("185 20", optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(20, optimized.reads);
    assertEquals(2, optimized.markers);
  }

  private Capture run(final String text, final int rows, final boolean enabled, final int evaluations) {
    final String previous = System.getProperty(ENABLED);
    System.setProperty(ENABLED, Boolean.toString(enabled));
    final Capture capture = new Capture();
    try (final BasicJsonDBStore actual = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final StringBuilder json = new StringBuilder(Math.max(2, rows * 100)).append('[');
      for (int i = 1; i <= rows; i++) {
        if (i > 1)
          json.append(',');
        json.append("{\"id\":")
            .append(i)
            .append(",\"cost\":")
            .append(i)
            .append(",\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}");
      }
      final JsonDBCollection collection = actual.create("bt", "contracts", json.append(']').toString());
      final JsonDBArray array = (JsonDBArray) collection.getDocument("contracts");
      final JsonDBArray counted = mock(JsonDBArray.class, withSettings().stubOnly().defaultAnswer(invocation -> {
        if (invocation.getMethod().getName().equals("at"))
          capture.reads++;
        try {
          return invocation.getMethod().invoke(array, invocation.getArguments());
        } catch (final InvocationTargetException exception) {
          throw exception.getCause();
        }
      }));
      final JsonDBCollection documents =
          mock(JsonDBCollection.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            if (invocation.getMethod().getName().equals("getDocument"))
              return counted;
            try {
              return invocation.getMethod().invoke(collection, invocation.getArguments());
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
      final BasicJsonDBStore store =
          mock(BasicJsonDBStore.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            if (invocation.getMethod().getName().equals("lookup"))
              return documents;
            try {
              return invocation.getMethod().invoke(actual, invocation.getArguments());
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final Query query = new Query(chain, text);
        capture.markers = markers(chain.getOptimizedAST());
        capture.planDump = chain.getOptimizedAST().toJSON();
        for (int i = 0; i < evaluations; i++) {
          final StringWriter output = new StringWriter();
          try (final PrintWriter writer = new PrintWriter(output)) {
            query.serialize(context, writer);
          }
          capture.answer = output.toString().trim();
        }
      }
      return capture;
    } finally {
      if (previous == null)
        System.clearProperty(ENABLED);
      else
        System.setProperty(ENABLED, previous);
    }
  }

  private static int materializedPipes(final JsonElement element) {
    final JsonObject node = element.getAsJsonObject();
    int count = 0;
    if (node.has("properties") && node.getAsJsonObject("properties").has(MARKER)) {
      assertEquals("PipeExpr", node.get("type").getAsString());
      assertTrue(node.getAsJsonObject("properties").get(MARKER).getAsBoolean());
      count++;
    }
    if (node.has("children")) {
      for (final JsonElement child : node.getAsJsonArray("children"))
        count += materializedPipes(child);
    }
    return count;
  }

  private static int markers(final AST node) {
    int count = Boolean.TRUE.equals(node.getProperty(MARKER))
        ? 1
        : 0;
    for (int i = 0; i < node.getChildCount(); i++)
      count += markers(node.getChild(i));
    return count;
  }

  private static final class Capture {
    long reads;
    int markers;
    String answer;
    String planDump;
  }
}
