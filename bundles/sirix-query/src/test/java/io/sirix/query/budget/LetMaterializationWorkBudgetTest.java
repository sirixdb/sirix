package io.sirix.query.budget;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Function;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalQueries;
import io.sirix.query.compiler.optimizer.SirixOptimizer;
import io.sirix.query.compiler.translator.SirixTranslator;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBCollection;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        Arguments.of("[exists($rows),(1,sum($rows))[?false()]][]", "true", 1),
        Arguments.of("[exists($rows),some $v in () satisfies sum($rows) gt 0][]", "true false", 1),
        Arguments.of("[exists($rows),every $v in () satisfies count($rows) gt 0][]", "true true", 1),
        Arguments.of("[exists($rows),some $v in (1,sum($rows)) satisfies $v eq 1][]", "true true", 1),
        Arguments.of("[exists($rows),every $v in (0,count($rows)) satisfies $v gt 0][]", "true false", 1),
        Arguments.of("[exists($rows),some $v in (),$w in sum($rows) satisfies true()][]", "true false", 1),
        Arguments.of("[exists($rows),every $v in (),$w in count($rows) satisfies false()][]", "true true", 1),
        Arguments.of("[exists($rows),some $v in 1,$w in () satisfies sum($rows) gt 0][]", "true false", 1),
        Arguments.of("[exists($rows),every $v in 1,$w in () satisfies count($rows) gt 0][]", "true true", 1),
        Arguments.of("[exists($rows),(1,2,sum($rows)) castable as xs:int][]", "true false", 1),
        Arguments.of("[exists($rows),(1,sum($rows)) = 1][]", "true true", 1),
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

  static Stream<Arguments> filteredConsumers() {
    return Stream.of(Arguments.of("[exists($rows),(1,sum($rows))[?1]][]", "true 1"),
        Arguments.of("[exists($rows),(1,2,count($rows))[?2]][]", "true 2"),
        Arguments.of("[exists($rows),(1,sum($rows))[?1][?1]][]", "true 1"),
        Arguments.of("[exists($rows),sum((1,sum($rows))[?1])][]", "true 1"),
        Arguments.of("[exists($rows),()[?sum($rows) gt 0]][]", "true"));
  }

  @ParameterizedTest
  @MethodSource("filteredConsumers")
  void filteredConsumersNeverIncreaseGenericSourceWork(final String result, final String answer) {
    final String query = "let $rows := (for $r in jn:doc('bt','contracts')[] return $r.cost) return " + result;
    final Capture optimized = run(query, 10000, true, 2);
    final Capture baseline = run(query, 10000, false, 2);
    assertEquals(answer, optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertTrue(baseline.reads > 0, "the source-read counter must observe item demand");
    assertTrue(baseline.reads <= 20002, "at most one first-item read and one full reduction per evaluation");
    assertTrue(optimized.reads <= baseline.reads, "filtering must not cause additional source consumption");
    assertEquals(0, optimized.markers);
  }

  static Stream<Arguments> deferredPrefixConsumers() {
    return Stream.of(Arguments.of("return (exists($rows),sum($rows))", "true", 1),
        Arguments.of("return (empty($rows),sum($rows))", "false", 1),
        Arguments.of("return (0,sum($rows),count($rows))", "0", 0),
        Arguments.of("return ((exists($rows)),(sum($rows),count($rows)))", "true", 1),
        Arguments.of("return (exists($rows),[sum($rows),count($rows)][])", "true", 1),
        Arguments.of("return ([exists($rows)][],sum($rows))", "true", 1),
        Arguments.of("return (0,[sum($rows),count($rows)][])", "0", 0),
        Arguments.of("return (exists($rows),count((sum($rows),count($rows))))", "true", 1),
        Arguments.of("return (exists($rows),{\"sum\":sum($rows),\"count\":count($rows)})", "true", 1),
        Arguments.of("for $outer in 1 to 2 return (exists($rows),sum($rows))", "true", 1),
        Arguments.of("return (exists($rows),(for $later in 1 return sum($rows)))", "true", 1));
  }

  @ParameterizedTest
  @MethodSource("deferredPrefixConsumers")
  void closingAfterTheFirstResultKeepsDeferredLocalSourcesLazy(final String body, final String answer,
      final int reads) {
    final Capture optimized = runLocalPrefix(body, true, 2, "");
    final Capture baseline = runLocalPrefix(body, false, 2, "");
    assertEquals(answer, optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(2L * reads, baseline.reads, "the disabled plan observes only the demanded source items");
    assertEquals(2L * reads, optimized.reads, "closing the result must not scan or buffer the 10,000-item source");
    assertEquals(0, optimized.markers);
    assertEquals(0, baseline.markers);
  }

  static Stream<Arguments> fullPrefixConsumers() {
    return Stream.of(Arguments.of("return (sum($rows),count($rows))", "50015000", 10000),
        Arguments.of("return (count($rows),exists($rows))", "10000", 10000),
        Arguments.of("return [exists($rows),sum($rows)][]", "true", 10001),
        Arguments.of("return [0,sum($rows),count($rows)][]", "0", 20000),
        Arguments.of("return [(exists($rows),sum($rows))][]", "true", 10001),
        Arguments.of("return [exists($rows),(sum($rows),count($rows))][]", "true", 20001),
        Arguments.of("return count((sum($rows),count($rows)))", "2", 20000),
        Arguments.of("return (sum($rows),(sum($rows),count($rows)))", "50015000", 10000));
  }

  static Stream<Arguments> partialPrefixConsumers() {
    return Stream.of(
        Arguments.of("return [exists($rows),sum($rows,?)][]", "true", 1),
        Arguments.of("return [exists($rows),min($rows,?)][]", "true", 1),
        Arguments.of("return [exists($rows),max($rows,?)][]", "true", 1),
        Arguments.of("return [exists($rows),sum((0,sum($rows)),?)][]", "true", 1),
        Arguments.of("return (exists($rows),sum($rows,?))", "true", 1),
        Arguments.of("return (sum($rows,?),sum($rows,?))", "function", 0),
        Arguments.of("return (sum((0,sum($rows)),?),sum((0,sum($rows)),?))", "function", 0),
        Arguments.of("return [count(sum($rows,?)),count(sum($rows,?))][]", "1", 0),
        Arguments.of("return [count(min($rows,?)),count(max($rows,?))][]", "1", 0),
        Arguments.of("return {\"first\":sum($rows,?),\"second\":sum($rows,?)} instance of object()", "true", 0),
        Arguments.of("let $partial := sum($rows,?) return (exists($rows),$partial)", "true", 1),
        Arguments.of("return [exists($rows),local:partial($rows)][]", "true", 1),
        Arguments.of("return [exists($rows),sum($rows),sum($rows,?)][]", "true", 10001));
  }

  @ParameterizedTest
  @MethodSource("partialPrefixConsumers")
  void partialReductionsNeverForceUndemandedLocalSourceWork(final String body, final String answer,
      final int reads) {
    final String declaration = "declare function local:partial($input) {sum($input,?)};";
    final Capture optimized = runLocalPrefix(body, true, 2, declaration);
    final Capture baseline = runLocalPrefix(body, false, 2, declaration);
    assertEquals(answer, optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(2L * reads, baseline.reads, "the disabled plan observes only executed reductions");
    assertEquals(2L * reads, optimized.reads, "creating a partial function must not traverse its bound argument");
    assertEquals(0, optimized.markers);
    assertEquals(0, baseline.markers);
  }

  @ParameterizedTest
  @MethodSource("fullPrefixConsumers")
  void fullDemandBeforeTheFirstResultStillMaterializesLocalSources(final String body, final String answer,
      final int reads) {
    final Capture optimized = runLocalPrefix(body, true, 2, "");
    final Capture baseline = runLocalPrefix(body, false, 2, "");
    assertEquals(answer, optimized.answer);
    assertEquals(baseline.answer, optimized.answer);
    assertEquals(2L * reads, baseline.reads, "the disabled plan proves full traversal before the first result");
    assertEquals(20000, optimized.reads, "one full source traversal per query evaluation");
    assertEquals(1, optimized.markers);
    assertEquals(0, baseline.markers);
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

  private static Capture runLocalPrefix(final String body, final boolean enabled, final int evaluations,
      final String declarations) {
    final String previous = System.getProperty(ENABLED);
    System.setProperty(ENABLED, Boolean.toString(enabled));
    final Capture capture = new Capture();
    try (final SirixQueryContext context = SirixQueryContext.create()) {
      final CompileChain chain = new CompileChain() {
        @Override
        protected Optimizer getOptimizer(final Map<QNm, Str> options) {
          return new SirixOptimizer(options, context.getNodeStore(), context.getJsonItemStore());
        }

        @Override
        protected Translator getTranslator(final Map<QNm, Str> options) {
          return new SirixTranslator(options) {
            @Override
            protected Expr arithmeticExpr(final AST node) {
              final Expr actual = super.arithmeticExpr(node);
              return mock(Expr.class, withSettings().stubOnly().defaultAnswer(invocation -> {
                final String method = invocation.getMethod().getName();
                if (method.equals("evaluate") || method.equals("evaluateToItem"))
                  capture.reads++;
                try {
                  return invocation.getMethod().invoke(actual, invocation.getArguments());
                } catch (final InvocationTargetException exception) {
                  throw exception.getCause();
                }
              }));
            }
          };
        }
      };
      final Query query = new Query(chain, declarations + " let $rows := (for $n in 1 to 10000 return $n + 1) " + body);
      capture.markers = markers(chain.getOptimizedAST());
      capture.reads = 0;
      for (int i = 0; i < evaluations; i++) {
        try (final Iter result = query.execute(context).iterate()) {
          final Item first = result.next();
          assertNotNull(first);
          capture.answer = first instanceof Function
              ? "function"
              : first.toString();
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
