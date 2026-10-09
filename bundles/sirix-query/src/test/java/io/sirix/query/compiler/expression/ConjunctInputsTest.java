package io.sirix.query.compiler.expression;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.Tuple;
import io.brackit.query.operator.TupleImpl;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

final class ConjunctInputsTest {
  @Test
  void currentDefaultProviderControlsBothGuardedEvaluationMethods(@TempDir final Path directory) {
    final AtomicInteger calls = new AtomicInteger();
    final Tuple tuple = new TupleImpl();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection stock = store.create("data", "rows", "{}");
      final JsonDBCollection custom =
          mock(JsonDBCollection.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            calls.incrementAndGet();
            throw new AssertionError("Admission must not invoke the default provider");
          }));
      final QNm[] names = new QNm[0];
      final GuardedConjunctExpr guarded =
          new GuardedConjunctExpr(Int32.ONE, Int32.ZERO, new ConjunctInputs(names, names, names, null, true));
      final GuardedConjunctExpr local =
          new GuardedConjunctExpr(Int32.ONE, Int32.ZERO, new ConjunctInputs(names, names, names, null, false));
      context.setDefaultJsonCollection(stock);
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
      context.setDefaultJsonCollection(custom);
      assertEquals(Int32.ZERO, guarded.evaluate(context, tuple));
      assertEquals(Int32.ZERO, guarded.evaluateToItem(context, tuple));
      assertEquals(Int32.ONE, local.evaluate(context, tuple));
      assertEquals(Int32.ONE, local.evaluateToItem(context, tuple));
      assertEquals(0, calls.get());
      clearDefaultJsonCollection(context);
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
    }
  }

  @Test
  void currentRegistrationsControlBothGuardedEvaluationMethods(@TempDir final Path directory) {
    final AtomicInteger calls = new AtomicInteger();
    final Tuple tuple = new TupleImpl();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection actual = store.create("data", "rows", "{}");
      final JsonDBCollection custom =
          mock(JsonDBCollection.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            calls.incrementAndGet();
            try {
              return invocation.getMethod().invoke(actual, invocation.getArguments());
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
      final QNm[] names = new QNm[0];
      final GuardedConjunctExpr guarded =
          new GuardedConjunctExpr(Int32.ONE, Int32.ZERO, new ConjunctInputs(names, names, names, null, true));
      final GuardedConjunctExpr local =
          new GuardedConjunctExpr(Int32.ONE, Int32.ZERO, new ConjunctInputs(names, names, names, null, false));
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
      store.addDatabase(custom, actual.getDatabase());
      calls.set(0);
      assertEquals(Int32.ZERO, guarded.evaluate(context, tuple));
      assertEquals(Int32.ZERO, guarded.evaluateToItem(context, tuple));
      assertEquals(Int32.ONE, local.evaluate(context, tuple));
      assertEquals(Int32.ONE, local.evaluateToItem(context, tuple));
      assertEquals(0, calls.get(), "admission must not call custom collections or open their documents");
      store.addDatabase(actual, actual.getDatabase());
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
      store.addDatabase(custom, actual.getDatabase());
      store.removeDatabase(actual.getDatabase());
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
      store.lookup("data");
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
      store.addDatabase(custom, actual.getDatabase());
      store.drop("data");
      assertEquals(Int32.ONE, guarded.evaluate(context, tuple));
      assertEquals(Int32.ONE, guarded.evaluateToItem(context, tuple));
    }
  }

  // Null clears the default provider so the guard can exercise its absent-provider case.
  @SuppressWarnings("NullAway")
  private static void clearDefaultJsonCollection(final SirixQueryContext context) {
    context.setDefaultJsonCollection(null);
  }
}
