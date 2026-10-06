package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

final class LetMaterializationTest {
  @ParameterizedTest
  @ValueSource(strings = {"current-dateTime()", "current-date()", "current-time()", "local:unknown($n)"})
  void unprovenCallsNeverMaterialize(final String expression) {
    final String text = "declare function local:unknown($n) {$n};" + " let $rows := (for $n in 1 to 3 return "
        + expression + ") return (count($rows),count($rows))";
    assertPlanAndAnswer(text, 0, "3 3");
  }

  @Test
  void effectfulLazyDependencyNeverMaterializes() {
    assertPlanAndAnswer(
        "declare function local:unknown($n) {$n};" + " let $source := (for $n in 1 to 3 return local:unknown($n))"
            + " let $rows := (for $n in $source return $n) return (count($rows),count($rows))",
        0, "3 3");
  }

  @Test
  void functionParametersDoNotProveTheirCallersLazySourcesPure() {
    assertPlanAndAnswer(
        "declare function local:use($input) {"
            + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))};" + "local:use(1 to 3)",
        0, "3 3");
  }

  static Stream<Arguments> parameterPaths() {
    return Stream.of("direct", "let-alias", "for-alias")
        .flatMap(shape -> Stream.of(Arguments.of(shape, false, false), Arguments.of(shape, true, false),
            Arguments.of(shape, true, true)));
  }

  @ParameterizedTest
  @MethodSource("parameterPaths")
  void functionParametersNeverUseSameNamedOuterPurity(final String shape, final boolean inline,
      final boolean outerLocal) {
    final AtomicInteger iterations = new AtomicInteger();
    final boolean composite = shape.equals("for-alias");
    final Sequence input = composite
        ? new ArrayObject(new QNm[] {new QNm("value")}, new Sequence[] {increasing(iterations)})
        : increasing(iterations);
    final String body = switch (shape) {
      case "direct" -> "let $rows := (for $n in $c return $n) return (sum($rows),sum($rows))";
      case "let-alias" -> "let $alias := $c let $rows := (for $n in $alias return $n)"
          + " return (sum($rows),sum($rows))";
      case "for-alias" -> "for $alias in $c let $rows := (for $n in 1 to 2 return $alias.value)"
          + " return (sum($rows),sum($rows))";
      default -> throw new IllegalArgumentException(shape);
    };
    final String text = "declare variable $c := 0; declare variable $input external; " + (inline
        ? (outerLocal
            ? "let $c := 0 "
            : "") + "let $f := function($c) {" + body + "} return $f($input)"
        : "declare function local:f($c) {" + body + "}; local:f($input)");
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, text);
      context.bind(new QNm("input"), input);
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals(composite
          ? "3 7"
          : "1 2", output.toString().trim());
      assertEquals(composite
          ? 4
          : 2, iterations.get());
      assertEquals(0, markers(chain.getOptimizedAST()));
    }
  }

  @Test
  void localBindingStillShadowsAFunctionParameterAndGlobal() {
    assertPlanAndAnswer("declare variable $c := 0; declare function local:f($c) {"
        + "let $c := 2 let $rows := (for $n in 1 to 2 return $c)"
        + " return (sum($rows),sum($rows))}; local:f(9)", 1, "4 4");
  }

  @Test
  void parameterReferencesDoNotEagerlyEvaluateAnUnusedOuterLet() {
    assertPlanAndAnswer("let $c := (for $n in 1 to 2 return $n div 0)"
        + " let $f := function($c) {($c,$c)} return $f(1)", 0, "1 1");
  }

  static Stream<Arguments> capturedPaths() {
    return Stream.of("let", "for", "let-alias", "for-alias", "global", "global-alias", "global-field")
        .flatMap(scope -> Stream.of(false, true)
            .flatMap(array -> (array
                ? Stream.of(false)
                : Stream.of(false, true)).map(stored -> Arguments.of(scope, array, stored))));
  }

  @ParameterizedTest
  @MethodSource("capturedPaths")
  void escapedCapturesKeepEvaluationPerReference(final String scope, final boolean array, final boolean stored,
      @TempDir final Path directory) {
    final AtomicInteger iterations = new AtomicInteger();
    final String literal = array
        ? "[0]"
        : "{\"value\":0}";
    final String source = stored
        ? "jn:doc('input','rows')"
        : array && scope.startsWith("for")
            ? "[" + literal + "]"
            : literal;
    final String field = array
        ? "[0]"
        : ".value";
    final String dependency = scope.endsWith("alias")
        ? "$alias" + field
        : scope.equals("global-field")
            ? "$alias"
            : "$a" + field;
    final String rows = "(let $rows := (for $n in 1 to 2 return " + dependency
        + ") return (sum($rows),sum($rows)))";
    final String text = switch (scope) {
      case "let", "for" -> scope + " $a " + (scope.equals("let") ? ":= " : "in ") + source
          + " return ($a," + rows + ")";
      case "let-alias", "for-alias" -> (scope.startsWith("let") ? "let $a := " : "for $a in ") + source
          + " let $alias := $a return ($a," + rows + ")";
      case "global" -> "declare variable $a := " + source + "; ($a," + rows + ")";
      case "global-alias", "global-field" -> "declare variable $a := " + source
          + "; declare variable $alias := $a" + (scope.equals("global-field") ? field : "")
          + "; ($a," + rows + ")";
      default -> throw new IllegalArgumentException(scope);
    };
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      if (stored)
        store.create("input", "rows", literal);
      final Query query = new Query(chain, text);
      try (final Iter result = query.execute(context).iterate()) {
        final Item first = result.next();
        if (array)
          assertInstanceOf(Array.class, first).replaceAt(0, increasing(iterations));
        else
          assertInstanceOf(Object.class, first).replace(new QNm("value"), increasing(iterations));
        iterations.set(0);
        assertEquals("3", result.next().toString());
        assertEquals("7", result.next().toString());
        assertEquals(4, iterations.get());
        assertNull(result.next());
      }
    }
  }

  @Test
  void effectfulGlobalInitializersDoNotProveLazyDependenciesPure() {
    assertPlanAndAnswer("declare variable $input := (for $n in 1 to 3 return current-dateTime());"
        + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))", 0, "3 3");
  }

  @Test
  void externalSequencesHaveNoStaticPurityProof() {
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, "declare variable $input external;"
          + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))");
      context.bind(new QNm("input"), Int32.ONE);
      assertEquals(0, markers(chain.getOptimizedAST()));
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals("1 1", output.toString().trim());
    }
  }

  @Test
  void positionalAllowingEmptyBindingsStillProveTheirActualSource() {
    assertPlanAndAnswer(
        "declare function local:source() { [{\"v\":1}][] };" + "for $r allowing empty at $position in local:source()"
            + " let $rows := (for $n in 1 to 2 return $r.v) return (sum($rows),count($rows))",
        0, "2 2");
  }

  @Test
  void allowingEmptyRebindingKeepsAnEmptyOuterTuple() {
    assertPlanAndAnswer("for $outer allowing empty in ()"
        + " let $rows := (for $n in 1 to 2 return $outer) return (count($rows),count($rows))", 1, "0 0");
  }

  @Test
  void externalDefaultsDoNotProveCallerBindingsPure() {
    assertPlanAndAnswer("declare variable $input external := 1 to 3;"
        + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))", 0, "3 3");
  }

  @Test
  void callerOverrideOfADeclaredGlobalSkipsMaterializationAtTheBinding() {
    final AtomicInteger iterations = new AtomicInteger();
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, "declare variable $input := 1 to 2;"
          + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))");
      assertEquals(1, markers(chain.getOptimizedAST()), "the default initializer is statically pure");
      context.bind(new QNm("input"), new LazySequence() {
        @Override
        public Iter iterate() {
          iterations.incrementAndGet();
          return new BaseIter() {
            private int next = 1;

            @Override
            public Item next() {
              return next <= 2
                  ? new Int32(next++)
                  : null;
            }

            @Override
            public void close() {}
          };
        }
      });
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals("2 2", output.toString().trim());
      assertEquals(2, iterations.get(), "a caller's unproven lazy value retains evaluation per reference");
    }
  }

  @Test
  void userFunctionShadowingAReadFunctionStillHasNoPurityProof() {
    assertPlanAndAnswer(
        "declare function jn:doc($collection,$resource) { 1 to 3 };"
            + "let $rows := (for $n in jn:doc('unused','unused') return $n)" + " return (count($rows),count($rows))",
        0, "3 3");
  }

  @Test
  void implicitContextArgumentsHaveNoPurityProof() {
    assertPlanAndAnswer("declare context item := 'value';"
        + "let $rows := (for $n in 1 to 3 return string()) return (count($rows),count($rows))", 0, "3 3");
  }

  @Test
  void customProvidersRetainPerReferenceReads(@TempDir final Path directory) {
    final AtomicInteger lookups = new AtomicInteger();
    try (final BasicJsonDBStore actual = BasicJsonDBStore.newBuilder().location(directory).build()) {
      actual.create("input", "rows", "[1,2,3]");
      final JsonDBStore custom = mock(JsonDBStore.class, withSettings().stubOnly().defaultAnswer(invocation -> {
        if (invocation.getMethod().getName().equals("lookup"))
          lookups.incrementAndGet();
        try {
          return invocation.getMethod().invoke(actual, invocation.getArguments());
        } catch (final InvocationTargetException exception) {
          throw exception.getCause();
        }
      }));
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(custom);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(custom)) {
        final Query query = new Query(chain,
            "let $rows := (for $n in jn:doc('input','rows')[] return $n)" + " return (count($rows),sum($rows))");
        assertEquals(1, markers(chain.getOptimizedAST()));
        lookups.set(0);
        final StringWriter output = new StringWriter();
        try (final PrintWriter writer = new PrintWriter(output)) {
          query.serialize(context, writer);
        }
        assertEquals("3 6", output.toString().trim());
        assertEquals(2, lookups.get(), "the purity of a custom provider's reads has not been established");
      }
    }
  }

  @Test
  void singleReferenceAndUnusedBindingStayLazy() {
    assertPlanAndAnswer("let $rows := (for $n in 1 to 3 return $n) return count($rows)", 0, "3");
    assertPlanAndAnswer("let $rows := (for $n in 1 to 3 return $n div 0) return 7", 0, "7");
  }

  @Test
  void shadowedNamesReferToTheirOwnBinding() {
    assertPlanAndAnswer("let $rows := (for $n in 1 to 3 return $n)"
        + " return (count($rows), (let $rows := (for $n in 1 to 2 return $n)" + " return (sum($rows),count($rows))))",
        1, "3 3 2");
  }

  @Test
  void updatingInitializerIsRejected() {
    // Exercise the optimizer with the actual parsed and analyzed updating expression.
    try (final SirixCompileChain chain = SirixCompileChain.create()) {
      new Query(chain,
          "let $rows := (for $n in 1 to 2 return jn:store('x','r','{}'))" + " return (count($rows),count($rows))");
      assertEquals(0, markers(chain.getOptimizedAST()));
    }
  }

  private static void assertPlanAndAnswer(final String text, final int markers, final String expected) {
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, text);
      assertEquals(markers, markers(chain.getOptimizedAST()));
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals(expected, output.toString().trim());
    }
  }

  private static Sequence increasing(final AtomicInteger iterations) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        final Item value = new Int32(iterations.incrementAndGet());
        return new BaseIter() {
          private boolean emitted;

          @Override
          public Item next() {
            if (emitted)
              return null;
            emitted = true;
            return value;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private static int markers(final AST node) {
    int count = Boolean.TRUE.equals(node.getProperty("SIRIX_MATERIALIZE_LET"))
        ? 1
        : 0;
    for (int i = 0; i < node.getChildCount(); i++)
      count += markers(node.getChild(i));
    return count;
  }
}
