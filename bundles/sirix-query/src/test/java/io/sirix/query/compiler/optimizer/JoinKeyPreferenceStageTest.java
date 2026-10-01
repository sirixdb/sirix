package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The join-key preference rule: Brackit's reversed selection order made the trailing comparison of
 * a where clause the join condition; with the rule the join is keyed on an equality, right-side
 * predicates sit inside the right input, everything else follows the join as a residual, and every
 * answer is byte-identical to the plan without the rule.
 */
final class JoinKeyPreferenceStageTest {

  private String previousEnabledProperty;

  private static final String CONTRACTS =
      "[{\"id\":1,\"pid\":10,\"cost\":5,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-07-01T00:00:00Z\"},"
          + "{\"id\":2,\"pid\":20,\"cost\":7,\"vf\":\"2024-03-01T00:00:00Z\",\"vt\":\"2024-09-01T00:00:00Z\"},"
          + "{\"id\":3,\"pid\":10,\"cost\":9,\"vf\":\"2024-06-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
          + "{\"id\":4,\"pid\":30,\"cost\":2,\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2024-02-01T00:00:00Z\"}]";
  private static final String PRODUCTS =
      "[{\"id\":10,\"category\":\"a\",\"retail\":20,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-08-01T00:00:00Z\"},"
          + "{\"id\":20,\"category\":\"b\",\"retail\":30,\"vf\":\"2024-04-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
          + "{\"id\":10,\"category\":\"c\",\"retail\":25,\"vf\":\"2024-08-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}]";
  private static final String PROLOG = "declare variable $L := xs:dateTime('2024-02-15T00:00:00Z');"
      + "declare variable $U := xs:dateTime('2024-10-15T00:00:00Z');" + "declare variable $C := " + CONTRACTS + ";"
      + "declare variable $P := " + PRODUCTS + ";";

  /** Q10's shape: equality first, window predicates on both sides, two interval inequalities last. */
  private static final String MIXED = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where $c.pid eq $p.id and xs:dateTime($c.vf) lt $U and $L lt xs:dateTime($c.vt)"
      + " and xs:dateTime($p.vf) lt $U and $L lt xs:dateTime($p.vt)"
      + " and xs:dateTime($c.vf) lt xs:dateTime($p.vt) and xs:dateTime($p.vf) lt xs:dateTime($c.vt)"
      + " let $category := $p.category, $margin := $p.retail - $c.cost"
      + " group by $category let $n := count($margin), $min := min($margin), $max := max($margin)"
      + " order by $category return {\"category\":$category,\"n\":$n,\"min\":$min,\"max\":$max}";
  private static final String EQUALITY_ONLY = PROLOG
      + "for $c in $C[] for $p in $P[] where $c.pid eq $p.id order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  private static final String INEQUALITY_ONLY = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where xs:dateTime($p.vf) lt xs:dateTime($c.vt) order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  private static final String TWO_EQUALITIES = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where $c.cost lt $p.retail and $c.pid eq $p.id and $c.id eq $p.id div 10"
      + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  private static final String THREE_WAY = PROLOG + "for $c in $C[] for $p in $P[] for $q in $P[]"
      + " where xs:dateTime($p.vf) lt xs:dateTime($c.vt) and $c.pid eq $p.id and $q.category ne $p.category and $q.id eq $c.pid"
      + " order by $c.id, $p.category, $q.category return {\"c\":$c.id,\"p\":$p.category,\"q\":$q.category}";
  private static final String LEFT_OUTER = PROLOG + "for $c in $C[]"
      + " let $m := (for $p in $P[] where xs:dateTime($p.vf) lt xs:dateTime($c.vt) and $p.id eq $c.pid return $p.category)"
      + " order by $c.id return {\"c\":$c.id,\"matches\":count($m)}";
  /** Q12's anti-join shape: one equality inside {@code empty(for … where … return …)}. */
  private static final String ANTI_JOIN = PROLOG + "for $c in $C[]"
      + " where empty(for $p in $P[] where $p.id eq $c.pid return $p) order by $c.id return $c.id";
  /** The same anti-join with a second, mixed predicate inside the nested where clause. */
  private static final String ANTI_JOIN_TWO_PREDICATES = PROLOG + "for $c in $C[]"
      + " where empty(for $p in $P[] where xs:dateTime($p.vf) gt xs:dateTime($c.vf) and $p.id eq $c.pid return $p)"
      + " order by $c.id return $c.id";
  private static final String NESTED_FOR =
      PROLOG + "for $c in $C[] for $p in $P[]" + " where $c.pid eq $p.id and $c.cost lt $p.retail"
          + " return count(for $q in $P[] where $q.id eq $p.id and $q.retail ge $c.cost return $q)";

  @BeforeEach
  void setUp() {
    previousEnabledProperty = System.getProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY);
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    if (previousEnabledProperty == null) {
      System.clearProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY);
    } else {
      System.setProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY, previousEnabledProperty);
    }
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("the mixed where clause is keyed on the equality, with the inequalities as the residual")
  void mixedWhereClauseIsKeyedOnTheEquality() throws IOException {
    final Plan plan = plan(MIXED);
    assertEquals(1, plan.joins.size(), "one join");
    final AST join = plan.joins.get(0);
    assertEquals("c.pid", deref(end(join.getChild(0))), "left join key");
    assertEquals("p.id", deref(end(join.getChild(1))), "right join key");
    assertTrue(selectionsBelow(join.getChild(1)) >= 1, "the right input filters products before the join");
    assertTrue(containsComparison(join.getChild(3), XQ.ValueCompLT), "the inequalities follow the join");
    assertEquals("{\"category\":\"a\",\"n\":2,\"min\":11,\"max\":15} "
        + "{\"category\":\"b\",\"n\":1,\"min\":23,\"max\":23} " + "{\"category\":\"c\",\"n\":1,\"min\":16,\"max\":16}",
        plan.answer.trim());
    final Plan without = planWithoutRule(MIXED);
    final AST joinWithout = without.joins.get(0);
    assertEquals("xs:dateTime", functionName(end(joinWithout.getChild(0))),
        "without the rule the join is keyed on the trailing dateTime inequality");
    assertEquals(without.answer, plan.answer, "answers are byte-identical");
  }

  @Test
  @DisplayName("an equality-only where clause is unchanged and exact")
  void equalityOnlyIsUnchanged() throws IOException {
    final Plan plan = plan(EQUALITY_ONLY);
    assertEquals("c.pid", deref(end(plan.joins.get(0).getChild(0))));
    assertEquals(planWithoutRule(EQUALITY_ONLY).answer, plan.answer);
  }

  @Test
  @DisplayName("an inequality-only where clause keeps its plan")
  void inequalityOnlyKeepsItsPlan() throws IOException {
    final Plan plan = plan(INEQUALITY_ONLY);
    final Plan without = planWithoutRule(INEQUALITY_ONLY);
    assertEquals(without.joins.size(), plan.joins.size());
    assertEquals("xs:dateTime", functionName(end(plan.joins.get(0).getChild(0))));
    assertEquals(without.answer, plan.answer);
  }

  @Test
  @DisplayName("with two equalities the first is the key and the second is a residual")
  void firstEqualityIsTheKey() throws IOException {
    final Plan plan = plan(TWO_EQUALITIES);
    final AST join = plan.joins.get(0);
    assertEquals("c.pid", deref(end(join.getChild(0))));
    assertEquals("p.id", deref(end(join.getChild(1))));
    assertTrue(containsComparison(join.getChild(3), XQ.ValueCompEQ), "the second equality is a residual");
    assertTrue(containsComparison(join.getChild(3), XQ.ValueCompLT), "the inequality is a residual");
    assertEquals(planWithoutRule(TWO_EQUALITIES).answer, plan.answer);
  }

  @Test
  @DisplayName("a three-way join is keyed on equalities at every level and exact")
  void threeWayJoinIsKeyedOnEqualities() throws IOException {
    final Plan plan = plan(THREE_WAY);
    assertTrue(plan.joins.size() >= 2, "two joins: " + plan.joins.size());
    for (final AST join : plan.joins) {
      assertTrue(end(join.getChild(0)).getType() == XQ.DerefExpr, "join keys are field derefs, not casts");
    }
    assertEquals(planWithoutRule(THREE_WAY).answer, plan.answer);
  }

  @Test
  @DisplayName("the anti-join shape (where empty(for … where … eq …)) is exact with and without the rule")
  void antiJoinShapeStaysExact() throws IOException {
    // Only contract 4 (pid 30) has no product with its pid.
    assertEquals("4", planWithoutRule(ANTI_JOIN).answer.trim(), "without the rule");
    assertEquals("4", plan(ANTI_JOIN).answer.trim(), "with the rule");
  }

  @Test
  @Disabled("a nested where clause with two predicates inside empty(…) answers nothing before this rule (every"
      + " contract is excluded: the nested pipeline's predicates are lost in the left-join conversion) — the same"
      + " filed nested-FLWOR bug as the let-bound shape; enable once it is fixed")
  @DisplayName("an anti-join with a mixed nested where clause excludes only the matched contracts")
  void antiJoinWithTwoNestedPredicates() throws IOException {
    // c1 and c3 have a later-starting product 10, c2 a later-starting product 20; only c4 (pid 30)
    // stays.
    assertEquals("4", planWithoutRule(ANTI_JOIN_TWO_PREDICATES).answer.trim(), "without the rule");
    assertEquals("4", plan(ANTI_JOIN_TWO_PREDICATES).answer.trim(), "with the rule");
  }

  @Test
  @DisplayName("a nested for in the return clause is exact with and without the rule")
  void nestedForInReturnStaysExact() throws IOException {
    // Pairs (c1,p10a) (c1,p10c) (c2,p20) (c3,p10a) (c3,p10c); products with the pair's id and retail >=
    // cost.
    assertEquals("2 2 1 2 2", plan(NESTED_FOR).answer.trim());
    assertEquals("2 2 1 2 2", planWithoutRule(NESTED_FOR).answer.trim());
  }

  @Test
  @Disabled("the let-bound nested FLWOR answers wrong counts before this rule (every contract gets 3 matches: the"
      + " where clause of the let-bound pipeline is lost) — the let-bound wrong-answer bug filed separately; enable"
      + " once it is fixed")
  @DisplayName("a let-bound outer-join shape counts the matching products")
  void letBoundOuterJoinCountsMatches() throws IOException {
    // c1: p10a only (p10c starts after c1 ends); c2: p20; c3: p10a and p10c; c4: none.
    final String expected =
        "{\"c\":1,\"matches\":1} {\"c\":2,\"matches\":1} {\"c\":3,\"matches\":2} {\"c\":4,\"matches\":0}";
    assertEquals(expected, planWithoutRule(LEFT_OUTER).answer.trim());
    assertEquals(expected, plan(LEFT_OUTER).answer.trim());
  }

  // ---------------------------------------------------------------------------------------------

  private record Plan(String answer, List<AST> joins) {
  }

  private static Plan plan(final String query) throws IOException {
    System.clearProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY);
    return compileAndRun(query);
  }

  private static Plan planWithoutRule(final String query) throws IOException {
    System.setProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY, "false");
    try {
      return compileAndRun(query);
    } finally {
      System.clearProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY);
    }
  }

  private static Plan compileAndRun(final String query) throws IOException {
    try (
        BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(out, false, StandardCharsets.UTF_8)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      final AST optimized = chain.getOptimizedAST();
      assertNotNull(optimized, "optimized AST");
      final List<AST> joins = new ArrayList<>();
      collect(optimized, XQ.Join, joins);
      return new Plan(out.toString(StandardCharsets.UTF_8), joins);
    }
  }

  private static void collect(final AST node, final int type, final List<AST> out) {
    if (node.getType() == type) {
      out.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      collect(node.getChild(i), type, out);
    }
  }

  /** The {@code End} expression of a join input ({@code Start → … → End(expr)}). */
  private static AST end(final AST start) {
    AST current = start;
    while (current.getType() != XQ.End) {
      current = current.getLastChild();
    }
    return current.getChild(0);
  }

  private static String deref(final AST expression) {
    assertEquals(XQ.DerefExpr, expression.getType(), "deref expected, got " + expression);
    final String variable = String.valueOf(expression.getChild(0).getValue());
    return variable.replaceAll(";\\d+$", "") + "." + expression.getChild(1).getValue();
  }

  private static String functionName(final AST expression) {
    assertEquals(XQ.FunctionCall, expression.getType(), "function call expected, got " + expression);
    return String.valueOf(expression.getValue());
  }

  private static int selectionsBelow(final AST node) {
    final List<AST> selections = new ArrayList<>();
    collect(node, XQ.Selection, selections);
    return selections.size();
  }

  private static boolean containsComparison(final AST node, final int comparisonType) {
    final List<AST> comparisons = new ArrayList<>();
    collect(node, XQ.ComparisonExpr, comparisons);
    for (final AST comparison : comparisons) {
      if (comparison.getChild(0).getType() == comparisonType) {
        return true;
      }
    }
    return false;
  }
}
