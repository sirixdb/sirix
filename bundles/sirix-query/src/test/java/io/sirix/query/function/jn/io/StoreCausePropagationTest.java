package io.sirix.query.function.jn.io;

import com.google.gson.stream.JsonReader;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.module.StaticContext;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.json.Options;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code jn:store} must surface the failure that actually aborted the ingestion.
 *
 * <p>The motivating case is a background snapshot flush that poisons the writer: the foreground
 * commit reports it as {@code SirixIOException("Async commit failed", backgroundThrowable)}, which
 * is the shape this test injects at the store boundary. Both wrapping layers inside {@link Store}
 * used to discard that throwable - the inner one built a message-only {@link QueryException} and
 * the outer one bound to the {@code (QNm, Object)} constructor, which renders the throwable into
 * the message instead of attaching it - so the real fault never reached any report and the failure
 * was undiagnosable.
 *
 * <p>The assertions walk {@link Throwable#getCause()} transitively and compare by identity. They
 * deliberately do not look at message text: a message can carry a rendered throwable while the
 * cause chain is empty, which is exactly the defect under test.
 */
final class StoreCausePropagationTest {

  private static final String COLLECTION = "cause-propagation";

  private static final String RESOURCE = "resource";

  /** {@link Store#execute} never reads the static context, so the reporting path needs none. */
  private static final StaticContext NO_STATIC_CONTEXT = null;

  /** The single-fragment {@code jn:store} arity, which creates the collection if it is absent. */
  private static Sequence[] storeArguments() {
    return new Sequence[] { new Str(COLLECTION), new Str(RESOURCE), new Str("{\"a\":1}") };
  }

  @Test
  void theBackgroundThrowableBehindAFailedAsyncCommitReachesTheCallerAsACause() {
    final IllegalStateException background =
        new IllegalStateException("snapshot append failed on the flush worker");
    final SirixIOException asyncCommitFailure = new SirixIOException("Async commit failed", background);
    final FailingJsonDBStore store = new FailingJsonDBStore(asyncCommitFailure);

    final QueryException thrown = assertThrows(QueryException.class,
        () -> new Store(true).execute(NO_STATIC_CONTEXT, SirixQueryContext.createWithJsonStore(store),
            storeArguments()));

    assertNotNull(thrown.getCause(), "jn:store must attach a cause, not render the failure into its message");
    assertTrue(causeChainContains(thrown, asyncCommitFailure),
        "the SirixIOException reporting the failed async commit must appear in the cause chain");
    assertTrue(causeChainContains(thrown, background),
        "the background throwable that actually poisoned the writer must be reachable from the cause chain");
  }

  /**
   * The chain must be intact for a checked-style storage fault too, so any store failure - not only
   * the async-commit shape - stays diagnosable.
   */
  @Test
  void aStoreFailureWithoutANestedCauseStillReachesTheCallerAsACause() {
    final SirixIOException storageFailure = new SirixIOException("could not append to the data file");
    final FailingJsonDBStore store = new FailingJsonDBStore(storageFailure);

    final QueryException thrown = assertThrows(QueryException.class,
        () -> new Store(true).execute(NO_STATIC_CONTEXT, SirixQueryContext.createWithJsonStore(store),
            storeArguments()));

    assertSame(storageFailure, deepestCause(thrown),
        "the storage failure must be the root of the reported cause chain");
  }

  private static boolean causeChainContains(final Throwable top, final Throwable target) {
    for (Throwable current = top.getCause(); current != null; current = current.getCause()) {
      if (current == target) {
        return true;
      }
    }
    return false;
  }

  private static Throwable deepestCause(final Throwable top) {
    Throwable current = top;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * A store whose only behaviour is to fail the resource creation {@code jn:store} performs, so the
   * test observes how {@link Store} reports a failure rather than how Sirix produces one.
   */
  private static final class FailingJsonDBStore implements JsonDBStore {

    private final RuntimeException failure;

    private FailingJsonDBStore(final RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public JsonDBCollection create(final String collName, final String optResName, final JsonReader json,
        final Object options) {
      throw failure;
    }

    @Override
    public JsonDBStore addDatabase(final JsonDBCollection collection, final Database<JsonResourceSession> database) {
      throw unsupported();
    }

    @Override
    public JsonDBStore removeDatabase(final Database<JsonResourceSession> database) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection lookup(final String name) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String name) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final Path path) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final Path path, final Object options) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection createFromPaths(final String collName, final Stream<Path> paths) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String optResName, final Path path) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String optResName, final Path path,
        final Object options) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String path) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String path, final Object options) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String optResName, final String json) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String optResName, final String json,
        final Object options) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final String optResName, final JsonReader json) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final Set<JsonReader> json) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection create(final String collName, final Set<JsonReader> json, final Object options) {
      throw unsupported();
    }

    @Override
    public JsonDBCollection createFromJsonStrings(final String collName, final Stream<Str> jsons) {
      throw unsupported();
    }

    @Override
    public void drop(final String name) {
      throw unsupported();
    }

    @Override
    public void makeDir(final String path) {
      throw unsupported();
    }

    @Override
    public void close() {
      // Nothing to release.
    }

    @Override
    public Options options() {
      throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException("not exercised by this test");
    }
  }
}
