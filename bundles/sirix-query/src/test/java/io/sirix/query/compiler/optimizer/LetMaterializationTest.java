package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

  private static int markers(final AST node) {
    int count = Boolean.TRUE.equals(node.getProperty("SIRIX_MATERIALIZE_LET"))
        ? 1
        : 0;
    for (int i = 0; i < node.getChildCount(); i++)
      count += markers(node.getChild(i));
    return count;
  }
}
