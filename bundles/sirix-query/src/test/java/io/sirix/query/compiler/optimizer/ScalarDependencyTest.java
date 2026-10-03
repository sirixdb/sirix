package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Sequence;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Isolated
final class ScalarDependencyTest {
  private static final String COMMIT = "declare variable $c := sdb:commit(jn:doc('inventory','rows')); ";
  private static final String ALIASES = "declare variable $a := $c + 0; declare variable $b := $a + 0; ";

  @TempDir
  Path directory;

  private static Stream<boolean[]> switches() {
    return Stream.of(new boolean[] {false}, new boolean[] {true});
  }

  static Stream<Arguments> conjunctions() {
    return Stream.of(new String[] {"xs:integer($c) gt 0", ""}, new String[] {"$c + 1 gt 0", ""},
        new String[] {"($c cast as xs:integer) gt 0", ""}, new String[] {"$c castable as xs:integer", ""},
        new String[] {"xs:integer($b) gt 0", ALIASES},
        new String[] {"let $alias := $b return xs:integer($alias) gt 0", ALIASES})
                 .flatMap(test -> switches().map(flags -> Arguments.of(flags[0], test[0], test[1])));
  }

  @ParameterizedTest
  @MethodSource("conjunctions")
  void cheapFalseConjunctCannotSuppressADefaultCommit(final boolean cheapFirst, final String expression,
      final String declarations) {
    withSwitch(cheapFirst, () -> {
      final Result result =
          evaluate(COMMIT + declarations + "declare variable $keep external; (" + expression + ") and $keep eq 1");
      assertEquals("false", result.answer);
      assertEquals(1, result.commits);
      final List<AST> ands = new ArrayList<>();
      collect(result.plan, XQ.AndExpr, ands);
      assertEquals(1, ands.size());
      final List<AST> refs = new ArrayList<>();
      collect(ands.get(0).getLastChild(), XQ.VariableRef, refs);
      assertEquals(new QNm("keep"), refs.get(0).getValue());
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pureDefaultsKeepReordering(final boolean cheapFirst) {
    withSwitch(cheapFirst, () -> {
      final Result result = evaluate("declare variable $limit := 3; declare variable $alias := $limit;"
          + " declare variable $date := xs:dateTime('2024-06-01T00:00:00Z');" + " let $r := (for $x in ["
          + "{\"id\":1,\"stamp\":\"2024-01-01T00:00:00Z\"}," + "{\"id\":2,\"stamp\":\"2024-01-01T00:00:00Z\"},"
          + "{\"id\":3,\"stamp\":\"2024-01-01T00:00:00Z\"}," + "{\"id\":4,\"stamp\":\"2024-01-01T00:00:00Z\"}][]"
          + " where xs:dateTime($x.stamp) le $date and $x.id le $alias return $x.id)" + " return (sum($r),sum($r))");
      assertEquals("6 6", result.answer);
      assertEquals(0, result.commits);
      final List<AST> ands = new ArrayList<>();
      collect(result.plan, XQ.AndExpr, ands);
      assertEquals(1, ands.size());
      assertEquals(cheapFirst
          ? 0
          : 20, CheapFirstConjunctStage.cost(ands.get(0).getChild(0)));
    });
  }

  private Result evaluate(final String query) {
    return evaluate(query, null);
  }

  private Result evaluate(final String query, final Sequence input) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("inventory", "rows", "{\"value\":10}");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("rows");
      assertEquals(1, session.getMostRecentRevisionNumber());
      ctx.bind(new QNm("keep"), Int32.ZERO);
      if (input != null) {
        ctx.bind(new QNm("c"), input);
      }
      final StringWriter out = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, writer);
      }
      return new Result(out.toString().trim(), session.getMostRecentRevisionNumber() - 1, chain.getOptimizedAST());
    }
  }

  private static void withSwitch(final boolean cheapFirst, final Runnable body) {
    final String previousCheap = System.getProperty(CheapFirstConjunctStage.ENABLED_PROPERTY);
    System.setProperty(CheapFirstConjunctStage.ENABLED_PROPERTY, Boolean.toString(cheapFirst));
    try {
      body.run();
    } finally {
      restore(CheapFirstConjunctStage.ENABLED_PROPERTY, previousCheap);
    }
  }

  private static void restore(final String name, final String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  private static void collect(final AST node, final int type, final List<AST> matches) {
    if (node.getType() == type) {
      matches.add(node);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      collect(node.getChild(i), type, matches);
    }
  }

  private record Result(String answer, int commits, AST plan) {
  }
}
