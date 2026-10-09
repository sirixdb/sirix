package io.sirix.query.budget;

import io.brackit.query.Query;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
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

/** Recompiling the same query must do zero parser, analyser, optimizer and translator work. */
@Isolated
final class CompiledQueryWorkBudgetTest {
  @TempDir
  Path directory;

  private static final WorkCounter PARSES = counter("compile.parses", CompileWorkStats::parses);
  private static final WorkCounter ANALYSES = counter("compile.analyses", CompileWorkStats::analyses);
  private static final WorkCounter OPTIMIZATIONS = counter("compile.optimizations", CompileWorkStats::optimizations);
  private static final WorkCounter TRANSLATIONS = counter("compile.translations", CompileWorkStats::translations);
  private static final List<WorkCounter> PHASES = List.of(PARSES, ANALYSES, OPTIMIZATIONS, TRANSLATIONS);

  @Test
  void secondExecutionDoesNoCompilerWork() throws Exception {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
         final var chain = SirixCompileChain.createWithJsonStore(store);
         final var interpreter = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final var context = SirixQueryContext.createWithJsonStore(store)) {
      store.create("inventory", "events", "[{\"n\":3},{\"n\":7},{\"n\":11}]");
      final String text = "let $rows := jn:doc('inventory','events')[]"
          + " for $r in $rows where $r.n gt 3 order by $r.n return $r.n + 1";
      final String expected = evaluate(interpreter, context, text);
      assertEquals("8 12", expected);
      final var cold = WorkCapture.of(PHASES).call(() -> evaluate(chain, context, text));
      assertEquals(expected, cold.result());
      for (final WorkCounter phase : PHASES) {
        cold.work().assertBetween(phase, 1, 4, "first compilation must observe live compiler work");
      }
      final var repeated = WorkCapture.of(PHASES).call(() -> evaluate(chain, context, text));
      assertEquals(expected, repeated.result());
      for (final WorkCounter phase : PHASES) {
        repeated.work().assertExactly(phase, 0, "repeated query must bypass the entire compiler");
      }
    }
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
