package io.sirix.query.budget;

import io.brackit.query.Query;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.budget.WorkReport;
import io.sirix.query.CompileWorkStats;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A repeated plan skips parse/analysis/optimization and translates fresh, bounded executable code. */
@Isolated
final class CompiledQueryWorkBudgetTest {
  @TempDir
  Path directory;

  private static final WorkCounter PARSES = counter("compile.parses", CompileWorkStats::parses);
  private static final WorkCounter ANALYSES = counter("compile.analyses", CompileWorkStats::analyses);
  private static final WorkCounter OPTIMIZATIONS = counter("compile.optimizations", CompileWorkStats::optimizations);
  private static final WorkCounter TRANSLATIONS = counter("compile.translations", CompileWorkStats::translations);
  private static final WorkCounter EXPRESSIONS = counter("compile.translationExpressions",
      CompileWorkStats::translatedExpressions);
  private static final List<WorkCounter> PHASES = List.of(PARSES, ANALYSES, OPTIMIZATIONS, TRANSLATIONS);
  private static final long EXPRESSION_DISPATCH_BUDGET = 20;

  @Test
  void repeatsOnlyTranslateFreshExpressionsWithFixedWork() throws Exception {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var interpreter = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      store.create("inventory", "events", "[{\"n\":3},{\"n\":7},{\"n\":11}]");
      final String text = "let $rows := jn:doc('inventory','events')[]"
          + " for $r in $rows where $r.n gt 3 order by $r.n return $r.n + 1";
      final String expected = evaluate(interpreter, context, text);
      assertEquals("8 12", expected);
      final var capture = WorkCapture.of(PHASES).and(EXPRESSIONS);
      final var cold = capture.call(() -> evaluate(chain, context, text));
      assertEquals(expected, cold.result());
      for (final WorkCounter phase : PHASES) {
        cold.work().assertBetween(phase, 1, 4, "first compilation must observe live compiler work");
      }
      cold.work().assertExactly(EXPRESSIONS, EXPRESSION_DISPATCH_BUDGET,
          "the cold control must observe the fixture's live translator work");
      final Query retained = new Query(chain, text);
      for (int repetition = 0; repetition < 5; repetition++) {
        final var repeated = capture.call(() -> evaluate(chain, context, text));
        assertEquals(expected, repeated.result());
        assertTranslationOnly(repeated.work());
        // Warm the retained facade once, then capture a further execution of the same Query.
        retained.serialize(context, new PrintWriter(new StringWriter()));
        final var reused = capture.call(() -> {
          final StringWriter output = new StringWriter();
          retained.serialize(context, new PrintWriter(output));
          return output.toString();
        });
        assertEquals(expected, reused.result());
        assertTranslationOnly(reused.work());
      }
    }
  }

  private static void assertTranslationOnly(final WorkReport work) {
    for (final WorkCounter phase : List.of(PARSES, ANALYSES, OPTIMIZATIONS)) {
      work.assertExactly(phase, 0, "a repeated plan must skip parser, analyser and optimizer");
    }
    work.assertExactly(TRANSLATIONS, 1, "each execution must perform one fresh translation phase");
    work.assertExactly(EXPRESSIONS, EXPRESSION_DISPATCH_BUDGET,
        "translation work must stay within the fixed fixture budget across repetitions");
  }

  private static WorkCounter counter(final String name, final LongSupplier reader) {
    return WorkCounter.gated(name, "one compiler phase invocation", reader,
        "-Dsirix.query.compileWorkDiag=true", () -> CompileWorkStats.ENABLED);
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    final StringWriter output = new StringWriter();
    new Query(chain, text).serialize(context, new PrintWriter(output));
    return output.toString();
  }
}
