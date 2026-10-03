package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.ModuleContext;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalQueries;
import io.sirix.query.json.BasicJsonDBStore;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@Isolated
final class CheapFirstConjunctTest {
  @TempDir
  Path directory;

  @Test
  void unknownCallsAreBarriersAndEqualCostsAreStable() {
    final AST first = new AST(XQ.FunctionCall, new QNm(Namespaces.XS_NSURI, "xs", "integer"));
    first.addChild(new AST(XQ.Str, new Str("7")));
    final AST barrier = new AST(XQ.FunctionCall, new QNm(Namespaces.LOCAL_NSURI, "local", "observe"));
    final AST last = new AST(XQ.VariableRef, new QNm("cheap"));
    final AST left = new AST(XQ.AndExpr);
    left.addChild(first);
    left.addChild(barrier);
    final AST root = new AST(XQ.AndExpr);
    root.addChild(left);
    root.addChild(last);
    final AST optimized = new CheapFirstConjunctStage().rewrite(new ModuleContext(), root);
    assertSame(root, optimized);
    assertSame(first, root.getChild(0).getChild(0));
    assertSame(barrier, root.getChild(0).getChild(1));
    assertSame(last, root.getChild(1));

    final Plan plan = run("for $r in [{\"a\":1,\"b\":2}][] where $r.a eq 1 and $r.b eq 2 return $r.a");
    assertEquals("1", plan.answer);
    final AST selection = only(plan.ast, XQ.Selection).getChild(0);
    final AST once = new CheapFirstConjunctStage().rewrite(new ModuleContext(), selection);
    assertSame(selection, once, "an equal-cost conjunction is left intact");
  }

  @ParameterizedTest
  @ValueSource(strings = {"eq", "=", "ne", "!=", "lt", "<", "le", "<=", "gt", ">", "ge", ">="})
  void literalComparisonPrecedesArithmeticAndCast(final String comparison) {
    final String query = "for $r in [{\"id\":1,\"v\":\"3\"},{\"id\":2,\"v\":\"4\"}][]"
        + " where xs:integer($r.v) gt 0 and $r.id + 2 gt 0 and $r.id " + comparison + " 2 return $r.id";
    final Plan plan = run(query);
    final String expected = switch (comparison) {
      case "eq", "=", "ge", ">=" -> "2";
      case "ne", "!=", "lt", "<" -> "1";
      case "le", "<=" -> "1 2";
      default -> "";
    };
    assertEquals(expected, plan.answer);
    final List<AST> terms = new ArrayList<>();
    flatten(only(plan.ast, XQ.Selection).getChild(0), terms);
    assertEquals(List.of(0, 10, 20), terms.stream().map(CheapFirstConjunctStage::cost).toList());
  }

  @Test
  void nestedPredicateFollowsCheapConjunctWithoutChangingItsBindings() {
    final Plan plan = run("for $r in [{\"id\":1,\"items\":[2,3]},{\"id\":2,\"items\":[4]}][]"
        + " where count(for $i in $r.items[] where $i gt 2 return $i) gt 0 and $r.id eq 1 return $r.id");
    assertEquals("1", plan.answer);
    final AST predicate = onlySelectionWithAnd(plan.ast);
    final List<AST> terms = new ArrayList<>();
    flatten(predicate, terms);
    assertEquals(0, CheapFirstConjunctStage.cost(terms.get(0)));
    assertEquals(30, CheapFirstConjunctStage.cost(terms.get(1)));
  }

  @ParameterizedTest
  @CsvSource(value = {"xs:integer($r.v) gt 0 or $r.id eq 9|1|20",
      "if ($r.id gt 0) then xs:integer($r.v) gt 0 else false()|1|20", "$r.v castable as xs:integer|1|20",
      "($r.v cast as xs:integer) gt 0|1|20", "some $n in 1 to 3 satisfies $n eq $r.id|1|30",
      "exists(for $n in 1 to 3 where $n eq $r.id return $n)|1|30"}, delimiter = '|')
  void compoundPurePredicatesRetainTheirHandComputedAnswers(final String predicate, final String expected,
      final int expensiveCost) {
    final Plan plan = run("for $r in [{\"id\":1,\"v\":\"3\"},{\"id\":2,\"v\":\"4\"}][]" + " where (" + predicate
        + ") and $r.id eq 1 return $r.id");
    assertEquals(expected, plan.answer);
    final List<AST> terms = new ArrayList<>();
    flatten(onlySelectionWithAnd(plan.ast), terms);
    assertEquals(List.of(0, expensiveCost), terms.stream().map(CheapFirstConjunctStage::cost).toList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void q10PlanAndAnswerAreUnchanged(final boolean cheapFirst) {
    final String contracts = """
        [{"id":1,"pid":10,"cost":5,"vf":"2024-01-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
         {"id":2,"pid":20,"cost":7,"vf":"2024-03-01T00:00:00Z","vt":"2024-09-01T00:00:00Z"},
         {"id":3,"pid":10,"cost":9,"vf":"2024-06-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":4,"pid":30,"cost":2,"vf":"2023-01-01T00:00:00Z","vt":"2024-02-01T00:00:00Z"},
         {"id":5,"pid":10,"cost":4,"vf":"2024-07-29T00:00:00Z","vt":"2024-12-31T00:00:00Z"},
         {"id":6,"pid":10,"cost":8,"vf":"2024-05-30T00:00:00Z","vt":"2024-06-01T00:00:00Z"}]
        """;
    final String products = """
        [{"id":10,"category":"a","retail":20,"vf":"2024-01-01T00:00:00Z","vt":"2024-08-01T00:00:00Z"},
         {"id":20,"category":"b","retail":30,"vf":"2024-04-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":10,"category":"c","retail":25,"vf":"2024-07-01T00:00:00Z","vt":"2024-07-29T00:00:00Z"},
         {"id":10,"category":"d","retail":40,"vf":"2024-05-30T00:00:00Z","vt":"2024-06-01T00:00:00Z"},
         {"id":10,"category":"x","retail":999,"vf":"2024-08-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":20,"category":"y","retail":999,"vf":"2023-01-01T00:00:00Z","vt":"2024-05-30T00:00:00Z"}]
        """;
    final String options = "{\"commitTimestamp\":\"2024-01-01T00:00:00Z\"}";
    runFresh("jn:store('bt','contracts','" + contracts + "',true()," + options + ")");
    runFresh("jn:store('bt','products','" + products + "',false()," + options + ")");
    final String query = BitemporalQueries.all().stream().filter(q -> q.index() == 10).findFirst().orElseThrow().text();
    final String expected = "{\"category\":\"a\",\"contracts\":3,\"min_margin\":11,\"max_margin\":15} "
        + "{\"category\":\"b\",\"contracts\":1,\"min_margin\":23,\"max_margin\":23} "
        + "{\"category\":\"c\",\"contracts\":1,\"min_margin\":16,\"max_margin\":16} "
        + "{\"category\":\"d\",\"contracts\":2,\"min_margin\":32,\"max_margin\":35}";
    final String oldCheapFirst = System.getProperty(CheapFirstConjunctStage.ENABLED_PROPERTY);
    try {
      System.setProperty(CheapFirstConjunctStage.ENABLED_PROPERTY, "true");
      final Plan enabled = runFresh(query);
      assertEquals(expected, enabled.answer);
      System.setProperty(CheapFirstConjunctStage.ENABLED_PROPERTY, Boolean.toString(cheapFirst));
      final Plan toggled = runFresh(query);
      assertEquals(expected, toggled.answer);
      assertSamePlan(enabled.ast, toggled.ast);
    } finally {
      restore(CheapFirstConjunctStage.ENABLED_PROPERTY, oldCheapFirst);
    }
  }

  private static void assertSamePlan(final AST expected, final AST actual) {
    assertEquals(expected.getType(), actual.getType(), "plan node type");
    final String expectedValue = expected.getValue() instanceof QNm name
        ? name.toString().replaceAll(";\\d+$", "")
        : String.valueOf(expected.getValue());
    final String actualValue = actual.getValue() instanceof QNm name
        ? name.toString().replaceAll(";\\d+$", "")
        : String.valueOf(actual.getValue());
    assertEquals(expectedValue, actualValue, "plan node value");
    assertEquals(expected.getChildCount(), actual.getChildCount(), "plan child count");
    for (int i = 0; i < expected.getChildCount(); i++) {
      assertSamePlan(expected.getChild(i), actual.getChild(i));
    }
  }

  @Test
  void indexScanCallsAreConjunctBarriers() {
    final AST scan = new AST(XQ.FunctionCall, new QNm(JSONFun.JSON_NSURI, "jn", "scan-valid-time-index"));
    scan.addChild(new AST(XQ.FunctionCall, new QNm(JSONFun.JSON_NSURI, "jn", "doc")));
    assertEquals(-1, CheapFirstConjunctStage.cost(scan), "conjunct ordering still never reorders across a scan");
  }

  private static void restore(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  private Plan runFresh(final String query) {
    final StringWriter out = new StringWriter();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, writer);
      writer.flush();
      return new Plan(out.toString().trim(), chain.getOptimizedAST());
    }
  }

  private Plan run(final String query) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      return run(store, query);
    }
  }

  private static Plan run(final BasicJsonDBStore store, final String query) {
    final StringWriter out = new StringWriter();
    try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, writer);
      writer.flush();
      return new Plan(out.toString().trim(), chain.getOptimizedAST());
    }
  }

  private static AST only(final AST node, final int type) {
    final List<AST> matches = new ArrayList<>();
    collect(node, type, matches);
    assertEquals(1, matches.size());
    return matches.get(0);
  }

  private static AST onlySelectionWithAnd(final AST node) {
    final List<AST> matches = new ArrayList<>();
    collect(node, XQ.Selection, matches);
    final List<AST> ands = matches.stream().map(n -> n.getChild(0)).filter(n -> n.getType() == XQ.AndExpr).toList();
    assertEquals(1, ands.size());
    return ands.get(0);
  }

  private static void collect(final AST node, final int type, final List<AST> matches) {
    if (node.getType() == type) {
      matches.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      collect(node.getChild(i), type, matches);
    }
  }

  private static void flatten(final AST node, final List<AST> out) {
    if (node.getType() == XQ.AndExpr) {
      assertEquals(2, node.getChildCount());
      flatten(node.getChild(0), out);
      flatten(node.getChild(1), out);
    } else {
      out.add(node);
    }
  }

  private record Plan(String answer, AST ast) {
  }
}
