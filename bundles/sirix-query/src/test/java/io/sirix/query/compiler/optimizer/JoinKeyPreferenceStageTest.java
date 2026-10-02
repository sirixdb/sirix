package io.sirix.query.compiler.optimizer;

import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.DefaultOptimizer;
import io.brackit.query.compiler.optimizer.Stage;
import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The join-key preference rule: the head of Brackit's pulled-up selection chain made a trailing
 * comparison the join condition; with the rule the join is keyed on an equality, right-side
 * predicates sit inside the right input, everything else follows the join as a residual, and every
 * answer is byte-identical to the plan without the rule.
 */
final class JoinKeyPreferenceStageTest {

  private String previousEnabledProperty;

  private static final String CONTRACTS =
      "[{\"id\":1,\"pid\":10,\"cost\":5,\"region\":\"eu\",\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-07-01T00:00:00Z\"},"
          + "{\"id\":2,\"pid\":20,\"cost\":7,\"region\":\"eu\",\"vf\":\"2024-03-01T00:00:00Z\",\"vt\":\"2024-09-01T00:00:00Z\"},"
          + "{\"id\":3,\"pid\":10,\"cost\":9,\"region\":\"us\",\"vf\":\"2024-06-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
          + "{\"id\":4,\"pid\":30,\"cost\":2,\"region\":\"eu\",\"vf\":\"2023-01-01T00:00:00Z\",\"vt\":\"2024-02-01T00:00:00Z\"}]";
  private static final String PRODUCTS =
      "[{\"id\":10,\"category\":\"a\",\"retail\":20,\"discount\":15,\"region\":\"eu\",\"items\":[1,9],\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-08-01T00:00:00Z\"},"
          + "{\"id\":20,\"category\":\"b\",\"retail\":30,\"discount\":23,\"region\":\"eu\",\"items\":[2,3],\"vf\":\"2024-04-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"},"
          + "{\"id\":10,\"category\":\"c\",\"retail\":25,\"discount\":16,\"region\":\"us\",\"items\":[4,8],\"vf\":\"2024-08-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}]";
  private static final String OFFSETS = "[{\"id\":1,\"b\":15},{\"id\":2,\"b\":23},{\"id\":3,\"b\":11}]";
  private static final String PROLOG = "declare variable $L := xs:dateTime('2024-02-15T00:00:00Z');"
      + "declare variable $U := xs:dateTime('2024-10-15T00:00:00Z');" + "declare variable $C := " + CONTRACTS + ";"
      + "declare variable $P := " + PRODUCTS + ";" + "declare variable $Q := " + OFFSETS + ";";

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
  /** A composite key: an unselective and a selective equality, either of which Brackit may head. */
  private static final String COMPOSITE_REGION_FIRST =
      PROLOG + "for $c in $C[] for $p in $P[]" + " where $c.region eq $p.region and $c.pid eq $p.id"
          + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /** The same composite key with its two conjuncts reversed, which heads the other equality. */
  private static final String COMPOSITE_REGION_LAST =
      PROLOG + "for $c in $C[] for $p in $P[]" + " where $c.pid eq $p.id and $c.region eq $p.region"
          + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /** Q12's anti-join shape: one equality inside {@code empty(for … where … return …)}. */
  private static final String ANTI_JOIN = PROLOG + "for $c in $C[]"
      + " where empty(for $p in $P[] where $p.id eq $c.pid return $p) order by $c.id return $c.id";
  /**
   * A conjunct that references only the right binding but opens its own scope for the quantified
   * variable, so it must still filter the build side rather than follow the join.
   */
  private static final String QUANTIFIED_RIGHT_ONLY =
      PROLOG + "for $c in $C[] for $p in $P[]" + " where (some $i in $p.items[] satisfies $i gt 5) and $c.pid eq $p.id"
          + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /** An equality whose right side opens its own scope; Brackit keys on it, so the rule must too. */
  private static final String NESTED_SCOPE_EQUALITY = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where $c.cost eq max(for $i in $p.items[] return $i) and $c.pid eq $p.id"
      + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  private static final String NESTED_FOR =
      PROLOG + "for $c in $C[] for $p in $P[]" + " where $c.pid eq $p.id and $c.cost lt $p.retail"
          + " return count(for $q in $P[] where $q.id eq $p.id and $q.retail ge $c.cost return $q)";
  /**
   * A {@code let}-bound chain whose head is already the equality, with a right-only conjunct behind
   * it: the key must stay put while the single-side conjunct moves to the build side.
   */
  private static final String LET_BOUND_EQUALITY_HEAD = PROLOG + "for $c in $C[] for $p in $P[] let $m := $p.id"
      + " where $m eq $c.pid and $m lt 25 order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /**
   * An equality whose one operand mixes two later bindings ({@code $p.retail - $q.b}) while the other
   * references only the earliest: {@code JoinRewriter} keys on it by building the mixed operand on the
   * build input, so the rule must leave that key alone rather than re-key onto {@code $q.id eq $c.id}
   * and enumerate the probe side's cross product.
   *
   * <p>
   * The assertions read the plan after the whole optimizer, not after {@code JoinRecognition}:
   * {@code CostBasedJoinReorder} puts the smaller relation on the build side, and here the
   * {@code $c.cost} side is the smaller one, so it ends up on child 1 and the mixed operand on child
   * 0 — the reverse of {@code MIXED}, whose two sides are estimated equal and so are left alone.
   * </p>
   */
  private static final String MIXED_SIDE_EQUALITY_THREE_BINDINGS =
      PROLOG + "for $c in $C[] for $p in $P[] for $q in $Q[]"
          + " where $c.cost eq $p.retail - $q.b and $q.id eq $c.id"
          + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";

  /**
   * An equality {@code JoinRewriter} cannot key on, because its operands reach the same latest
   * binding and the written probe operand begins later: {@code sortScopes} gives {@code [p]} against
   * {@code [c, p]}, the maxima tie so no orientation swap happens, and {@code p >= c} exhausts the
   * probe side. Hoisting it above the inequality would only copy it into the join's right input,
   * which then references {@code $c} and rebuilds the whole product side per contract.
   */
  private static final String UNKEYABLE_MIXED_SIDE_EQUALITY = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where xs:dateTime($p.vf) lt xs:dateTime($c.vt) and $p.retail eq $c.cost + $p.discount"
      + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /**
   * The same tie written the other way round, so the mixed operand lands on the PROBE side.
   * {@code JoinRewriter} admits this one and compiles {@code $c.cost + $p.retail} into a left input
   * rooted above {@code ForBind($p)}, where {@code $p} is unbound — promoting it would replace a
   * query that answers today with a plan that does not compile at all.
   */
  private static final String PROBE_SIDE_MIXED_EQUALITY = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where xs:dateTime($p.vf) lt xs:dateTime($c.vt) and $c.cost + $p.retail eq $p.retail + 5"
      + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /**
   * The same probe-side mixed equality in HEAD position, with a qualifying equality behind it. This
   * is the one shape where declining to promote changes the plan: {@code JoinRewriter} would key on
   * the head and emit a left input that cannot resolve {@code $p}, so the rule keys on
   * {@code $c.pid eq $p.id} instead and the query answers. There is deliberately no rule-off
   * comparison — that plan is the Brackit failure this change neither causes nor owes a fix.
   */
  private static final String PROBE_SIDE_MIXED_EQUALITY_AT_HEAD = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where $c.cost + $p.retail eq $p.retail + 5 and $c.pid eq $p.id"
      + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";
  /**
   * An equality whose build operand begins at a scope the predicate opens itself, so the binding
   * {@code JoinRewriter} would root the right input at is a descendant of the selection rather than
   * an ancestor and it leaves the selection alone.
   */
  private static final String PREDICATE_LOCAL_SCOPE_EQUALITY = PROLOG + "for $c in $C[] for $p in $P[]"
      + " where $c.cost lt $p.retail and $c.cost + $p.retail eq count(for $i in (1 to 30) return $i)"
      + " order by $c.id, $p.category return {\"c\":$c.id,\"p\":$p.category}";

  /**
   * The finding's example for the documented error exposure: the {@code id:99} product never joins,
   * but its {@code w} is not castable. Hoisting the cast to the build side makes it run on that row.
   */
  private static final String RAISING_BUILD_SIDE_CONJUNCT = "declare variable $C := [{\"pid\":10}];"
      + "declare variable $P := [{\"id\":10,\"w\":\"5\"},{\"id\":99,\"w\":\"oops\"}];"
      + "for $c in $C[] for $p in $P[] where $c.pid eq $p.id and xs:integer($p.w) gt 0"
      + " return {\"c\":$c.pid,\"p\":$p.id}";

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
    assertEquals(2, comparisonsBelow(join.getChild(1), XQ.ValueCompLT),
        "both $p-only window predicates filter the build side before the join");
    assertEquals(XQ.AndExpr, onlySelection(join.getChild(1)).getChild(0).getType(),
        "Brackit's own PredicateMerge collapses them into one conjunction");
    assertEquals(2, comparisonsBelow(join.getChild(3), XQ.ValueCompLT),
        "only the two interval inequalities, which reference both bindings, follow the join");
    assertEquals(XQ.AndExpr, onlySelection(join.getChild(3)).getChild(0).getType(),
        "Brackit's own PredicateMerge collapses the residuals into one conjunction");
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
  @DisplayName("with two equalities the key Brackit already picked is kept")
  void twoEqualitiesKeepTheKeyBrackitPicked() throws IOException {
    assertKeptKey(TWO_EQUALITIES);
  }

  @Test
  @DisplayName("a composite key is keyed exactly as it is without the rule")
  void compositeKeyKeepsTheRuleOffKey() throws IOException {
    final Plan plan = assertKeptKey(COMPOSITE_REGION_FIRST);
    assertEquals("c.region", deref(end(plan.joins.get(0).getChild(0))), "the equality at the chain head");
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":2,\"p\":\"b\"} {\"c\":3,\"p\":\"c\"}", plan.answer.trim());
  }

  @Test
  @DisplayName("the same composite key with its conjuncts reversed is also keyed as it is without the rule")
  void reversedCompositeKeyKeepsTheRuleOffKey() throws IOException {
    // The reversed clause heads the chain with the other equality, so the two keys really do differ.
    final Plan plan = assertKeptKey(COMPOSITE_REGION_LAST);
    assertEquals("c.pid", deref(end(plan.joins.get(0).getChild(0))), "the equality at the chain head");
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":2,\"p\":\"b\"} {\"c\":3,\"p\":\"c\"}", plan.answer.trim());
  }

  @Test
  @DisplayName("a conjunct that opens its own scope still filters the build side")
  void quantifiedRightOnlyConjunctStaysOnTheBuildSide() throws IOException {
    final Plan plan = plan(QUANTIFIED_RIGHT_ONLY);
    final Plan without = planWithoutRule(QUANTIFIED_RIGHT_ONLY);
    final AST join = plan.joins.get(0);
    assertEquals("c.pid", deref(end(join.getChild(0))), "left join key");
    assertEquals(selectionsBelow(without.joins.get(0).getChild(1)), selectionsBelow(join.getChild(1)),
        "the quantified conjunct filters the build side, exactly as without the rule");
    assertEquals(0, selectionsBelow(join.getChild(3)), "it is not demoted to a post-join filter");
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":1,\"p\":\"c\"} {\"c\":3,\"p\":\"a\"} {\"c\":3,\"p\":\"c\"}",
        plan.answer.trim());
    assertEquals(without.answer, plan.answer, "answers are byte-identical");
  }

  @Test
  @DisplayName("an equality whose side opens its own scope keeps the key Brackit picked")
  void nestedScopeEqualityKeepsTheRuleOffKey() throws IOException {
    final Plan plan = assertKeptKey(NESTED_SCOPE_EQUALITY);
    assertEquals("{\"c\":3,\"p\":\"a\"}", plan.answer.trim());
  }

  @Test
  @DisplayName("the stage is installed directly before Brackit's join recognition")
  void theStageIsInstalledBeforeJoinRecognition() {
    assertTrue(DefaultOptimizer.JOIN_DETECTION, "Brackit join detection is on by default");
    final List<Stage> stages = new SirixOptimizer(null, null, null).getStages();
    int preference = -1;
    int joinRecognition = -1;
    for (int i = 0; i < stages.size(); i++) {
      if (stages.get(i) instanceof JoinKeyPreferenceStage) {
        preference = i;
      } else if ("JoinRecognition".equals(stages.get(i).getClass().getSimpleName())) {
        joinRecognition = i;
      }
    }
    assertTrue(joinRecognition >= 0, "Brackit still names its join-recognition stage JoinRecognition");
    assertEquals(joinRecognition - 1, preference, "the join-key preference stage runs directly before it");
  }

  @Test
  @DisplayName("the off switch keeps the stage, and so its anchor requirement, out of the pipeline")
  void theOffSwitchInstallsNoStage() {
    System.setProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY, "false");
    try {
      for (final Stage stage : new SirixOptimizer(null, null, null).getStages()) {
        assertFalse(stage instanceof JoinKeyPreferenceStage, "no join-key preference stage is installed");
      }
    } finally {
      System.clearProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY);
    }
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
  @DisplayName("a let-bound chain keeps its equality key and pushes its single-side conjunct")
  void letBoundEqualityHeadKeepsItsKey() throws IOException {
    final Plan plan = assertKeptKey(LET_BOUND_EQUALITY_HEAD);
    final AST join = plan.joins.get(0);
    assertEquals(1, selectionsBelow(join.getChild(1)), "the single-side conjunct filters the build side");
    assertEquals(0, selectionsBelow(join.getChild(3)), "it is not left behind as a post-join filter");
    final AST joinWithout = planWithoutRule(LET_BOUND_EQUALITY_HEAD).joins.get(0);
    assertEquals(0, selectionsBelow(joinWithout.getChild(1)), "without the rule the build side is unfiltered");
    assertEquals(1, selectionsBelow(joinWithout.getChild(3)), "the conjunct follows the join instead");
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":1,\"p\":\"c\"} {\"c\":2,\"p\":\"b\"} {\"c\":3,\"p\":\"a\"} "
        + "{\"c\":3,\"p\":\"c\"}", plan.answer.trim());
  }

  @Test
  @DisplayName("a mixed-side equality spanning two later bindings keeps the key Brackit picked")
  void mixedSideEqualityOverThreeBindingsKeepsTheRuleOffKey() throws IOException {
    final Plan plan = assertKeptKey(MIXED_SIDE_EQUALITY_THREE_BINDINGS);
    assertEquals(XQ.ArithmeticExpr, end(plan.joins.get(0).getChild(0)).getType(),
        "the mixed operand stays one side of the key");
    assertEquals("c.cost", deref(end(plan.joins.get(0).getChild(1))), "and the earliest binding the other");
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":2,\"p\":\"b\"} {\"c\":3,\"p\":\"a\"}", plan.answer.trim());
  }

  @Test
  @DisplayName("a conjunct pushed to the build side is evaluated on rows that never join")
  void pushedBuildSideConjunctRunsOnNonJoiningRows() throws IOException {
    assertEquals("{\"c\":10,\"p\":10}", planWithoutRule(RAISING_BUILD_SIDE_CONJUNCT).answer.trim(),
        "without the rule the cast follows the join, so it only ever sees the joined pair");
    final QueryException raised =
        assertThrows(QueryException.class, () -> plan(RAISING_BUILD_SIDE_CONJUNCT),
            "with the rule the cast filters the build side, where the non-joining row reaches it");
    assertEquals(ErrorCode.ERR_INVALID_VALUE_FOR_CAST, raised.getCode(),
        "the documented exposure is a dynamic error, not a wrong answer");
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
  @DisplayName("an equality JoinRewriter cannot key on is left where it is, not hoisted")
  void unkeyableMixedSideEqualityIsLeftAlone() throws IOException {
    assertUnchangedPlan(UNKEYABLE_MIXED_SIDE_EQUALITY);
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":2,\"p\":\"b\"} {\"c\":3,\"p\":\"c\"}",
        plan(UNKEYABLE_MIXED_SIDE_EQUALITY).answer.trim());
  }

  @Test
  @DisplayName("an equality whose probe operand reads the build binding is left where it is")
  void probeSideMixedEqualityIsLeftAlone() throws IOException {
    assertUnchangedPlan(PROBE_SIDE_MIXED_EQUALITY);
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":1,\"p\":\"b\"}", plan(PROBE_SIDE_MIXED_EQUALITY).answer.trim());
  }

  @Test
  @DisplayName("a rejected equality at the chain head is re-keyed onto the equality behind it")
  void probeSideMixedEqualityAtHeadIsRekeyedOntoTheEqualityBehindIt() throws IOException {
    final Plan plan = plan(PROBE_SIDE_MIXED_EQUALITY_AT_HEAD);
    assertEquals(1, plan.joins.size(), "one join");
    assertEquals("c.pid", deref(end(plan.joins.get(0).getChild(0))), "keyed on the equality behind the head");
    assertEquals("p.id", deref(end(plan.joins.get(0).getChild(1))), "build side of that key");
    assertEquals("{\"c\":1,\"p\":\"a\"} {\"c\":1,\"p\":\"c\"}", plan.answer.trim());
  }

  @Test
  @DisplayName("an equality rooted at a predicate-local scope is left where it is, not hoisted")
  void predicateLocalScopeEqualityIsLeftAlone() throws IOException {
    assertUnchangedPlan(PREDICATE_LOCAL_SCOPE_EQUALITY);
    assertEquals("{\"c\":1,\"p\":\"c\"}", plan(PREDICATE_LOCAL_SCOPE_EQUALITY).answer.trim());
  }

  @Test
  @DisplayName("only an explicit false switches the rule off")
  void onlyAnExplicitFalseSwitchesTheRuleOff() {
    try {
      for (final String off : new String[] {"false", "FALSE", " false "}) {
        System.setProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY, off);
        assertFalse(JoinKeyPreferenceStage.enabled(), "switched off by '" + off + "'");
      }
      for (final String on : new String[] {"true", "TRUE", "1", "yes", "on", ""}) {
        System.setProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY, on);
        assertTrue(JoinKeyPreferenceStage.enabled(), "still on for '" + on + "'");
      }
      System.setProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY, "yes");
      assertTrue(new SirixOptimizer(null, null, null).getStages().stream()
          .anyMatch(JoinKeyPreferenceStage.class::isInstance), "the stage stays installed for a non-false value");
    } finally {
      System.clearProperty(JoinKeyPreferenceStage.ENABLED_PROPERTY);
    }
    assertTrue(JoinKeyPreferenceStage.enabled(), "on by default");
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * A chain the rule must not touch: same join key, and every selection still on the same side of
   * the join, so nothing is hoisted into the right input to be rebuilt per enclosing tuple.
   */
  private static void assertUnchangedPlan(final String query) throws IOException {
    final Plan plan = plan(query);
    final Plan without = planWithoutRule(query);
    assertEquals(without.joins.size(), plan.joins.size(), "join count");
    for (int i = 0; i < plan.joins.size(); i++) {
      final AST join = plan.joins.get(i);
      final AST joinWithout = without.joins.get(i);
      assertEquals(keySignature(joinWithout), keySignature(join), "join key");
      assertEquals(selectionsBelow(joinWithout.getChild(1)), selectionsBelow(join.getChild(1)),
          "selections in the join's right input");
      assertEquals(selectionsBelow(joinWithout.getChild(3)), selectionsBelow(join.getChild(3)),
          "selections following the join");
    }
    assertEquals(without.answer, plan.answer, "answers are byte-identical");
  }

  /**
   * The rule must never re-key a join away from any equality Brackit itself already picked: it keys
   * on the equality nearest the head of the selection chain, which is the one {@code JoinRewriter}
   * reaches first — a mixed-side operand included.
   */
  private static Plan assertKeptKey(final String query) throws IOException {
    final Plan plan = plan(query);
    final Plan without = planWithoutRule(query);
    assertEquals(without.joins.size(), plan.joins.size(), "join count");
    assertEquals(keySignature(without.joins.get(0)), keySignature(plan.joins.get(0)), "join key");
    assertEquals(without.answer, plan.answer, "answers are byte-identical");
    return plan;
  }

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

  /** The two keyed expressions of a join, as a structural signature comparable across plans. */
  private static String keySignature(final AST join) {
    return signature(end(join.getChild(0))) + " eq " + signature(end(join.getChild(1)));
  }

  private static String signature(final AST node) {
    final StringBuilder out = new StringBuilder(64);
    signature(node, out);
    return out.toString();
  }

  private static void signature(final AST node, final StringBuilder out) {
    out.append(XQ.NAMES[node.getType()]);
    if (node.getValue() != null) {
      out.append('(').append(String.valueOf(node.getValue()).replaceAll(";\\d+$", "")).append(')');
    }
    if (node.getChildCount() > 0) {
      out.append('[');
      for (int i = 0; i < node.getChildCount(); i++) {
        if (i > 0) {
          out.append(", ");
        }
        signature(node.getChild(i), out);
      }
      out.append(']');
    }
  }

  private static AST onlySelection(final AST node) {
    final List<AST> selections = new ArrayList<>();
    collect(node, XQ.Selection, selections);
    assertEquals(1, selections.size(), "exactly one selection");
    return selections.get(0);
  }

  private static int selectionsBelow(final AST node) {
    final List<AST> selections = new ArrayList<>();
    collect(node, XQ.Selection, selections);
    return selections.size();
  }

  private static int comparisonsBelow(final AST node, final int comparisonType) {
    final List<AST> comparisons = new ArrayList<>();
    collect(node, XQ.ComparisonExpr, comparisons);
    int count = 0;
    for (final AST comparison : comparisons) {
      if (comparison.getChild(0).getType() == comparisonType) {
        count++;
      }
    }
    return count;
  }
}
