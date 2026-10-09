package io.sirix.query.compiler.expression;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.optimizer.LetMaterializationStage;
import io.sirix.query.compiler.optimizer.SirixOptimizer;
import io.sirix.query.compiler.translator.SirixPipelineStrategy;
import io.sirix.query.compiler.translator.SirixRowMaterializeExpr;
import io.sirix.query.compiler.translator.SirixTranslator;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@Isolated
final class EagerLetMaterializationTest {
  @TempDir
  Path directory;

  static Stream<Arguments> coveredRows() {
    return Stream.of(1, 1000).flatMap(rows -> Stream.of(false, true).map(enabled -> Arguments.of(rows, enabled)));
  }

  @ParameterizedTest
  @MethodSource("coveredRows")
  void coveredRowBuffersAreReusedWithoutChangingOutputOrBindingLifetime(final int rows, final boolean enabled) {
    final VectorizedExecutor previousExecutor = SequentialPipelineStrategy.getVectorizedExecutor();
    final String previousProperty = System.getProperty(LetMaterializationStage.ENABLED_PROPERTY);
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    System.setProperty(LetMaterializationStage.ENABLED_PROPERTY, Boolean.toString(enabled));
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain setup = SirixCompileChain.createWithJsonStore(store)) {
      final StringBuilder json = new StringBuilder(rows * 16).append('[');
      for (int row = 1; row <= rows; row++) {
        if (row > 1)
          json.append(',');
        json.append("{\"v\":").append(row).append('}');
      }
      final JsonDBCollection collection = store.create("data", "rows", json.append(']').toString());
      new Query(setup, "let $doc := jn:doc('data','rows')"
          + " let $index := jn:create-projection-index($doc,'/[]',('/[]/v'),('long'))" + " return sdb:commit($doc)")
                                                                                                                    .evaluate(
                                                                                                                        context);
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("rows");
      session.getNodeTrx().orElseThrow().close();
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 1);
      try {
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        final AtomicReference<Sequence> served = new AtomicReference<>();
        final AtomicReference<Sequence> materialized = new AtomicReference<>();
        final Query query = new Query(observingChain(store, served, materialized),
            "let $rows := (for $r in jn:doc('data','rows')[] return {\"v\":$r.v})"
                + " return {\"sum\":sum($rows.v),\"count\":count($rows)}");
        Sequence previous = null;
        for (int evaluation = 0; evaluation < 2; evaluation++) {
          final long before = SirixVectorizedExecutor.rowMaterializeServedCount();
          final StringWriter output = new StringWriter();
          query.serialize(context, new PrintWriter(output));
          assertEquals("{\"sum\":" + (long) rows * (rows + 1) / 2 + ",\"count\":" + rows + "}", output.toString());
          assertEquals(1, SirixVectorizedExecutor.rowMaterializeServedCount() - before,
              "the source must execute the real covered-row serving route once per binding evaluation");
          final Sequence buffered = requireNonNull(served.get());
          assertEquals(ItemSequence.class, buffered.getClass());
          assertEquals(rows, buffered.size().intValue());
          assertNotSame(previous, buffered, "each evaluation receives its own serving buffer");
          if (enabled)
            assertSame(buffered, requireNonNull(materialized.get()), "the binding must reuse the serving buffer");
          else
            assertNull(materialized.get());
          previous = buffered;
        }
      } finally {
        executor.close();
      }
    } finally {
      SequentialPipelineStrategy.setVectorizedExecutor(previousExecutor);
      if (previousProperty == null)
        System.clearProperty(LetMaterializationStage.ENABLED_PROPERTY);
      else
        System.setProperty(LetMaterializationStage.ENABLED_PROPERTY, previousProperty);
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
    }
  }

  private static CompileChain observingChain(final BasicJsonDBStore store, final AtomicReference<Sequence> served,
      final AtomicReference<Sequence> materialized) {
    return new CompileChain() {
      @Override
      protected Optimizer getOptimizer(final Map<QNm, Str> options) {
        return new SirixOptimizer(options, mock(XmlDBStore.class), store);
      }

      @Override
      protected Translator getTranslator(final Map<QNm, Str> options) {
        final SirixPipelineStrategy strategy = new SirixPipelineStrategy();
        return new SirixTranslator(options, (node, compiler) -> {
          final Expr actual = strategy.compilePipeExpr(node, compiler);
          return actual instanceof SirixRowMaterializeExpr
              ? observe(actual, served)
              : actual;
        }) {
          @Override
          protected Expr anyExpr(final AST node) {
            final Expr actual = super.anyExpr(node);
            return actual instanceof MaterializeExpr
                ? observe(actual, materialized)
                : actual;
          }
        };
      }
    };
  }

  private static Expr observe(final Expr actual, final AtomicReference<Sequence> observed) {
    return mock(Expr.class, withSettings().stubOnly().defaultAnswer(invocation -> {
      try {
        final var result = invocation.getMethod().invoke(actual, invocation.getArguments());
        if (invocation.getMethod().getName().equals("evaluate") && result instanceof Sequence sequence)
          observed.set(sequence);
        return result;
      } catch (final InvocationTargetException exception) {
        throw requireNonNull(exception.getCause());
      }
    }));
  }
}
