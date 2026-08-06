package io.sirix.query.scan;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.QueryContext;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for the indirection that lets a compiled query follow the revision it should be
 * reading — see {@link RevisionTrackingExecutor}.
 */
final class RevisionTrackingExecutorTest {

  private static final String DB = "revision-tracking-db";
  private static final String RES = "records.jn";

  /**
   * The delegation trap: a method added to {@link VectorizedExecutor} later would be inherited here
   * as the interface default ("unsupported") instead of being forwarded. At the entry points
   * brackit substitutes at translate time that is not a lost fast path but a failed query, and
   * nothing else in the suite would notice — every existing test would still pass. So the full set
   * is checked structurally, and a new interface method breaks the build instead of the user.
   */
  @Test
  void everyExecutorMethodIsForwarded() {
    final Set<String> forwarded = new HashSet<>();
    for (final Method m : RevisionTrackingExecutor.class.getDeclaredMethods()) {
      if (!m.isSynthetic()) {
        forwarded.add(signature(m));
      }
    }
    final List<String> missing = new ArrayList<>();
    for (final Method m : VectorizedExecutor.class.getDeclaredMethods()) {
      if (m.isSynthetic() || Modifier.isStatic(m.getModifiers())) {
        continue;
      }
      if (!forwarded.contains(signature(m))) {
        missing.add(signature(m));
      }
    }
    assertTrue(missing.isEmpty(),
               "RevisionTrackingExecutor must forward every VectorizedExecutor method; missing: " + missing);
  }

  private static String signature(final Method m) {
    return m.getName() + Arrays.toString(m.getParameterTypes());
  }

  /**
   * Before anything has ever resolved, every entry point declines rather than throwing — the
   * translator then builds the generic pipeline, which is the only correct thing to do when there
   * is nothing to serve from.
   */
  @Test
  void anUnresolvableExecutorDeclinesEverything() {
    final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(() -> null);
    assertNull(executor.lastResolved());
    assertFalse(executor.canExecute(null));
    assertFalse(executor.acceptsSource(SourceRef.document("db", "res", SourceRef.LATEST_REVISION)));
    assertFalse(executor.supportsSortedScan());
    assertFalse(executor.supportsMultiKeyGroupBy());
    assertNull(executor.executeAggregate(null, new String[] { "[]" }, "sum", "age"));
    assertNull(executor.executeGroupByCount(null, new String[] { "[]" }, "age"));
    assertNull(executor.executePredicateCount(null, new String[] { "[]" }, null));
  }

  /**
   * A resolver that stops answering — the compile chain that owned the executor was closed — must
   * leave the last resolved one in place. A compiled query outliving its chain then keeps answering
   * from the revision it last saw, instead of failing at an entry point that has no fallback.
   */
  @Test
  void theLastResolvedExecutorSurvivesAResolverThatStopsAnswering() throws Exception {
    final Path dbDir = Files.createTempDirectory("sirix-revision-tracking-");
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbDir.resolve(DB)));
      try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
        db.createResource(ResourceConfiguration.newBuilder(RES).build());
        try (final var session = db.beginResourceSession(RES)) {
          try (final var wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"age\":1},{\"age\":2}]"));
            wtx.commit();
          }
          final SirixVectorizedExecutor resolved =
              new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          try {
            final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>();
            final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(answer::get);
            assertNull(executor.lastResolved());

            answer.set(resolved);
            assertTrue(executor.canExecute(null));
            assertSame(resolved, executor.lastResolved(), "the resolved executor must be remembered");

            answer.set(null);
            assertTrue(executor.canExecute(null),
                       "a resolver that stops answering must keep serving from the last one");
            assertSame(resolved, executor.lastResolved());
          } finally {
            resolved.close();
          }
        }
      }
    } finally {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  /**
   * The gate pins what it admitted so the scan that follows serves one revision — and the pin dies
   * with that scan. A pin that outlived its evaluation would sit on the thread until the next gate
   * call: the next execution of the same compiled query would then be served by the executor of the
   * PREVIOUS one, which after a commit is exactly the stale-revision answer this class exists to
   * remove.
   */
  @Test
  void theGatePinDoesNotOutliveTheScanItAdmitted() throws Exception {
    withResource((session, executorFactory) -> {
      final SirixVectorizedExecutor gated = executorFactory.get();
      final SirixVectorizedExecutor afterCommit = executorFactory.get();
      try {
        final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>(gated);
        final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(answer::get);
        final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);
        final QueryContext evaluation = new BrackitQueryContext();

        assertTrue(executor.acceptsSource(source, evaluation), "the gate must admit its own document");
        assertSame(gated, executor.lastResolved());

        // Within the admitted evaluation the pin holds, even though the resolver has moved on.
        answer.set(afterCommit);
        assertTrue(executor.canExecute(evaluation));
        assertSame(gated, executor.lastResolved(), "the gated scan must serve from what the gate admitted");

        // The scan consumes the pin; everything after it re-resolves.
        executor.executeAggregate(evaluation, new String[] { "[]" }, "count", "age");
        assertTrue(executor.canExecute(evaluation));
        assertSame(afterCommit, executor.lastResolved(), "the pin must not outlive the scan that consumed it");
      } finally {
        gated.close();
        afterCommit.close();
      }
    });
  }

  /**
   * Belt and braces for the same property: a pin is bound to the evaluation that took it, so even
   * one left behind cannot answer for a LATER execution — which carries its own
   * {@link QueryContext}.
   */
  @Test
  void aPinCannotServeADifferentEvaluation() throws Exception {
    withResource((session, executorFactory) -> {
      final SirixVectorizedExecutor gated = executorFactory.get();
      final SirixVectorizedExecutor afterCommit = executorFactory.get();
      try {
        final AtomicReference<SirixVectorizedExecutor> answer = new AtomicReference<>(gated);
        final RevisionTrackingExecutor executor = new RevisionTrackingExecutor(answer::get);
        final SourceRef source = SourceRef.document(DB, RES, SourceRef.LATEST_REVISION);

        assertTrue(executor.acceptsSource(source, new BrackitQueryContext()));
        assertSame(gated, executor.lastResolved());

        answer.set(afterCommit);
        assertTrue(executor.canExecute(new BrackitQueryContext()));
        assertSame(afterCommit, executor.lastResolved(),
                   "a pin from an earlier evaluation must not answer for a new one");
      } finally {
        gated.close();
        afterCommit.close();
      }
    });
  }

  /** A null resolver is a programming error, not a decline. */
  @Test
  void aNullResolverIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> new RevisionTrackingExecutor(null));
  }

  /** What a test does with an open resource session and a factory for executors bound to it. */
  private interface ResourceTest {
    void run(JsonResourceSession session, Supplier<SirixVectorizedExecutor> executorFactory) throws Exception;
  }

  /** Creates a throwaway single-revision resource, runs {@code test} against it, and removes it. */
  private static void withResource(final ResourceTest test) throws Exception {
    final Path dbDir = Files.createTempDirectory("sirix-revision-tracking-");
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbDir.resolve(DB)));
      try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
        db.createResource(ResourceConfiguration.newBuilder(RES).build());
        try (final var session = db.beginResourceSession(RES)) {
          try (final var wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"age\":1},{\"age\":2}]"));
            wtx.commit();
          }
          test.run(session, () -> new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber()));
        }
      }
    } finally {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }
}
