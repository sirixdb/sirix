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

import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
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

  /** A null resolver is a programming error, not a decline. */
  @Test
  void aNullResolverIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> new RevisionTrackingExecutor(null));
  }
}
