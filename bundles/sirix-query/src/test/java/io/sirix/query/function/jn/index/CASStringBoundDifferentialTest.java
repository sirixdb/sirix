package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.compiler.CompileChain;
import io.sirix.JsonTestHelper;
import io.sirix.query.AbstractJsonTest;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CASStringBoundDifferentialTest extends AbstractJsonTest {
  private static final String SOURCE = "jn:doc('json-path1','mydoc.jn')";
  private static final String[] LITERALS = {"\uD800", "\uDC00", "\uD800x", "\uDC00x"};

  @BeforeEach
  void loadFixture() {
    query("jn:store('json-path1','mydoc.jn','["
        + "{\"title\":\"\",\"alias\":\"a\"},{\"title\":\"?\",\"alias\":\"a\"},"
        + "{\"title\":\"a\",\"alias\":\"a\"},{\"title\":\"a\",\"alias\":\"a\"},"
        + "{\"title\":\"！\",\"alias\":\"a\"},{\"title\":\"𐐀\",\"alias\":\"a\"}]')");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lossyOrderingBoundsMatchTheInterpreter(final boolean multiPath) {
    createIndex(multiPath);
    final String[] operators = {"<", "<=", ">", ">="};
    final String[] comparisons = {"lt", "le", "gt", "ge"};
    for (final String literal : LITERALS) {
      for (int i = 0; i < operators.length; i++) {
        final String predicate = "$o.title " + comparisons[i] + " '" + literal + "'";
        final String plain = "for $o in " + SOURCE + "[] where " + predicate + " return $o.title";
        final String indexed = scan(literal, operators[i]);
        assertEquals(i < 2 ? "4" : "2", run("count(" + plain + ")", false));
        assertDifferential(indexed, plain);
        assertDifferential(SOURCE + "[][?$$.title " + comparisons[i] + " '" + literal + "'].title", plain);
        assertEquals(run("count(" + plain + ")", false), run("count(" + plain + ")", true));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lossyOneSidedAndTwoSidedRangeBoundsMatchTheInterpreter(final boolean multiPath) {
    createIndex(multiPath);
    for (final String literal : LITERALS) {
      for (final boolean inclusive : new boolean[] {false, true}) {
        assertRange(null, literal, true, inclusive);
        assertRange(literal, null, inclusive, true);
        assertRange("?", literal, inclusive, inclusive);
        assertRange(literal, "\uFFFF", inclusive, inclusive);
      }
    }
    assertRange("\uD800", "\uDC00", true, true);
    assertRange("\uDC00", "\uD800", true, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void losslessBoundsKeepTheirResults(final boolean multiPath) {
    createIndex(multiPath);
    for (final String literal : new String[] {"?", "a", "！", "𐐀"}) {
      assertDifferential(scan(literal, "<="),
          "for $o in " + SOURCE + "[] where $o.title le '" + literal + "' return $o.title");
      assertRange("a", literal, true, false);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"\uD800", "\uDC00"})
  void lossyBoundsWithMultipleRequestedPathsMatchTheInterpreter(final String literal) {
    createIndex(true);
    for (final String op : new String[] {"<", ">="}) {
      final String comparison = op.equals("<") ? "lt" : "ge";
      final String indexed = "let $doc := " + SOURCE + " return jn:scan-cas-index($doc,"
          + "jn:find-cas-index($doc,'xs:string','/[]/title'),'" + literal + "','" + op + "',())";
      final String plain = "for $o in " + SOURCE + "[] for $v in ($o.title, $o.alias) where $v "
          + comparison + " '" + literal + "' return $v";
      assertDifferential(indexed, plain);
    }
  }

  private void createIndex(final boolean multiPath) {
    final String paths = multiPath ? "('/[]/title','/[]/alias')" : "'/[]/title'";
    query("let $doc := " + SOURCE + " let $idx := jn:create-cas-index($doc,'xs:string'," + paths
        + ") return sdb:commit($doc)");
  }

  private static String scan(final String literal, final String op) {
    return "let $doc := " + SOURCE + " return jn:scan-cas-index($doc,"
        + "jn:find-cas-index($doc,'xs:string','/[]/title'),'" + literal + "','" + op + "','/[]/title')";
  }

  private void assertRange(final String min, final String max, final boolean includeMin, final boolean includeMax) {
    final String minExpr = min == null ? "()" : "'" + min + "'";
    final String maxExpr = max == null ? "()" : "'" + max + "'";
    final String indexed = "let $doc := " + SOURCE + " return jn:scan-cas-index-range($doc,"
        + "jn:find-cas-index($doc,'xs:string','/[]/title')," + minExpr + "," + maxExpr + ","
        + includeMin + "()," + includeMax + "(),'/[]/title')";
    final String lower = "$o.title " + (includeMin ? "ge" : "gt") + " " + minExpr;
    final String upper = "$o.title " + (includeMax ? "le" : "lt") + " " + maxExpr;
    final String predicate = min == null ? upper : max == null ? lower : lower + " and " + upper;
    final String plain = "for $o in " + SOURCE + "[] where " + predicate + " return $o.title";
    assertDifferential(indexed, plain);
    assertDifferential(SOURCE + "[][?" + predicate.replace("$o", "$$") + "].title", plain);
  }

  private void assertDifferential(final String indexed, final String plain) {
    assertEquals(run(ordered(plain), false), run(ordered(indexed), true), indexed);
  }

  private static String ordered(final String expression) {
    return "for $v in (" + expression + ") order by $v return $v";
  }

  private String run(final String expression, final boolean optimized) {
    try (final var store = BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        new Query(optimized ? chain : new CompileChain(), expression).serialize(context, writer);
      }
      return output.toString();
    }
  }
}
