/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import io.sirix.io.fault.FaultInjectingStorage;
import io.sirix.io.fault.FaultInjectingWriter;
import io.sirix.io.fault.FaultInjectingWriter.Point;
import io.sirix.io.fault.FaultInjectingWriter.SimulatedCrashException;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * How a synchronous commit reports a failure on each side of the commit point.
 *
 * <p>
 * A commit crosses its commit point when {@code Writer#writeUberPageReference} returns: at that
 * moment revision N is durable and acknowledged. The steps after it — forgetting the spill file,
 * closing durable read pages, releasing the page guard, clearing the transaction-intent log,
 * deleting the recovery marker — are cleanup of a revision that already exists.
 * {@code spec: TXN-307} makes a failure in that cleanup a success for the caller: it is logged, the
 * resource is flagged for repair and the writer becomes terminal, but the commit is NOT reported as
 * failed, because the data IS persisted. {@code spec: TXN-305} is the contrast: a failure known to
 * precede the commit point means nothing was committed and leaves the transaction usable.
 *
 * <p>
 * The failures are injected through {@link FaultInjectingStorage}, which puts a
 * {@link FaultInjectingWriter} in front of the real {@code FILE_CHANNEL} writer of a live resource
 * session, so these are real commits of a real resource.
 */
@Isolated
@Timeout(30)
final class PostDurableCleanupFailureTest {

  private static final String RESOURCE = "resource";

  private Path workRoot;

  @BeforeEach
  void setUp() throws IOException {
    workRoot = Files.createTempDirectory("sirix-post-durable-cleanup-").toRealPath();
    FaultInjectingStorage.install(workRoot);
  }

  @AfterEach
  void tearDown() throws IOException {
    AbstractNodeTrxImpl.asyncCommitTestHook = null;
    FaultInjectingStorage.uninstall();
    deleteRecursive(workRoot);
  }

  /**
   * A cleanup failure after the commit point: the revision is durable, so the commit must be reported
   * as the success it is, the writer must be terminal rather than running, and the revision must be
   * readable afterwards. {@code spec: TXN-307}
   */
  @Test
  void cleanupFailureAfterTheCommitPointReportsTheCommitAsSucceeded() {
    final Path databasePath = createDatabase();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int bootstrapRevision = session.getMostRecentRevisionNumber();

      final JsonNodeTrx wtx = session.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);

      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      injector.injectAt(Point.BEFORE_DISCARD_SPILLED_WRITES, 1);

      // The commit point was crossed, so this must NOT throw — before the fix it reported the
      // simulated cleanup failure as a commit failure (#1380).
      wtx.commit();

      assertFalse(injector.isArmedFor(Point.BEFORE_DISCARD_SPILLED_WRITES),
          "the post-durable cleanup injection never fired, so this run proves nothing");

      // Terminal: the writer does not go back to RUNNING, so it can neither be reused nor retried
      // into a second copy of the same change.
      assertThrows(IllegalStateException.class,
          () -> wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"b\":2}"), JsonNodeTrx.Commit.NO));

      // Nothing uncommitted is left, so closing the terminal writer is clean.
      wtx.close();

      assertEquals(bootstrapRevision + 1, session.getMostRecentRevisionNumber(),
          "the durable revision must be the session's most recent one");
    }

    // The revision survives the session and is readable through a fresh open, whose admission also
    // runs the repair the cleanup failure flagged.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(1, session.getMostRecentRevisionNumber());
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        assertEquals(1, rtx.getChildCount(), "the committed object must still hold its single field");
      }
    }
  }

  /**
   * The contrast: a storage failure before the beacon is written commits nothing, and the transaction
   * keeps the rollback path that makes it usable again. {@code spec: TXN-305}
   *
   * <p>
   * TXN-305 asks for a usable writer, but for a failure this late it asks for the rollback path
   * rather than immediate reuse: page serialization already retired the epoch's buffers, so the
   * mutation path is only safe again once rollback has reconstructed the lineage. What this asserts
   * is the contrast that the post-durable branch must not reach: the ordinary failure is thrown, no
   * revision number is consumed, the work is still counted as dirty, and the writer is not terminal.
   */
  @Test
  void failureBeforeTheCommitPointCommitsNothingAndKeepsTheTransactionUsable() {
    final Path databasePath = createDatabase();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int bootstrapRevision = session.getMostRecentRevisionNumber();

      final JsonNodeTrx wtx = session.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);

      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      injector.injectAt(Point.BEFORE_UBER_PAGE_REFERENCE_WRITE, 1);

      assertThrows(SimulatedCrashException.class, wtx::commit);

      assertFalse(injector.isArmedFor(Point.BEFORE_UBER_PAGE_REFERENCE_WRITE),
          "the pre-beacon injection never fired, so this run proves nothing");

      assertEquals(bootstrapRevision, session.getMostRecentRevisionNumber(), "nothing may be durable");

      // The work still counts as uncommitted: only a commit that crossed the commit point clears
      // the dirty counter, so the post-durable branch cannot have been taken here.
      assertThrows(SirixUsageException.class, wtx::close);

      // And the writer is not terminal — it still accepts the rollback TXN-305 preserves.
      wtx.rollback();
      wtx.close();

      assertEquals(bootstrapRevision, session.getMostRecentRevisionNumber());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"commit", "rollback", "revert", "truncate", "maintenance", "atomic", "bulk", "record"})
  void durableCleanupFailureIrreversiblyRejectsEveryLifecycleBoundary(final String operation) {
    final Path databasePath = createDatabase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final JsonNodeTrx writer = session.beginNodeTrx();
      writer.insertArrayAsFirstChild();
      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      injector.injectAt(Point.BEFORE_DISCARD_SPILLED_WRITES, 1);
      assertSame(writer, writer.commit());
      assertFalse(injector.isArmedFor(Point.BEFORE_DISCARD_SPILLED_WRITES));
      final AbstractNodeTrxImpl<?, ?, ?, ?, ?> internal = (AbstractNodeTrxImpl<?, ?, ?, ?, ?>) writer;
      final Runnable action = switch (operation) {
        case "commit" -> writer::commit;
        case "rollback" -> writer::rollback;
        case "revert" -> () -> writer.revertTo(0);
        case "truncate" -> () -> writer.truncateTo(0);
        case "maintenance" -> () -> internal.runStorageMaintenance(() -> {
          throw new AssertionError("terminal writer admitted history maintenance");
        });
        case "atomic" -> internal::beginAtomicOperation;
        case "bulk" -> internal::checkAccessAndCommitBulk;
        case "record" -> () -> internal.bulkAccountMutations(1);
        default -> throw new AssertionError(operation);
      };
      assertThrows(IllegalStateException.class, action::run);
      assertThrows(IllegalStateException.class, writer::commit);
      assertThrows(IllegalStateException.class, writer::rollback);
      assertDoesNotThrow(writer::close);
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
    assertReopenedArray(databasePath);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ordinary", "bulk", "record"})
  void automaticRotationStopsBeforeAccountingAnotherMutation(final String mode) {
    final Path databasePath = createDatabase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final JsonNodeTrx writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN);
      writer.insertArrayAsFirstChild();
      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      injector.injectAt(Point.BEFORE_DISCARD_SPILLED_WRITES, 1);
      if (mode.equals("bulk")) {
        ((InternalNodeTrx<?>) writer).setBulkInsertion(true);
      }
      if (mode.equals("record")) {
        final AbstractNodeTrxImpl<?, ?, ?, ?, ?> internal = (AbstractNodeTrxImpl<?, ?, ?, ?, ?>) writer;
        assertThrows(IllegalStateException.class, () -> internal.bulkAccountMutations(1));
      } else {
        assertThrows(IllegalStateException.class, () -> writer.insertStringValueAsFirstChild("uncommitted"));
      }
      assertFalse(injector.isArmedFor(Point.BEFORE_DISCARD_SPILLED_WRITES));
      assertDoesNotThrow(writer::close);
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
    assertReopenedArray(databasePath);
  }

  @ParameterizedTest
  @ValueSource(strings = {"close-before", "close-after", "open"})
  void synchronousSuccessorFailurePreservesDurableSuccess(final String failure) {
    final Path databasePath = createDatabase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final JsonNodeTrx writer = session.beginNodeTrx();
      writer.insertArrayAsFirstChild();
      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      final Point point = failure.equals("close-before") ? Point.BEFORE_CLOSE : Point.AFTER_CLOSE;
      if (failure.equals("open")) {
        FaultInjectingStorage.failNextWriterCreation();
      } else {
        injector.injectAt(point, 1);
      }
      assertSame(writer, writer.commit());
      if (failure.equals("open")) {
        assertFalse(FaultInjectingStorage.isWriterCreationFailureArmed());
      } else {
        assertFalse(injector.isArmedFor(point));
      }
      assertTerminal(writer);
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) session).getCommitFile()));
      assertDoesNotThrow(writer::close);
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
    assertReopenedArray(databasePath);
  }

  @ParameterizedTest
  @ValueSource(strings = {"after-publication", "before-diff-publication"})
  void synchronousPublicationFailurePreservesDurableSuccess(final String failingStage) {
    final int expectedRevision = failingStage.equals("before-diff-publication") ? 2 : 1;
    final Path databasePath = createDatabase(true);
    final AtomicBoolean fired = new AtomicBoolean();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      if (expectedRevision == 2) {
        try (final JsonNodeTrx seed = session.beginNodeTrx()) {
          seed.commit();
        }
      }
      final JsonNodeTrx writer = session.beginNodeTrx();
      writer.insertArrayAsFirstChild();
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals(failingStage) && fired.compareAndSet(false, true)) {
          throw new SimulatedCrashException(stage);
        }
      };
      assertSame(writer, writer.commit());
      assertTrue(fired.get());
      assertTerminal(writer);
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) session).getCommitFile()));
      assertDoesNotThrow(writer::close);
      assertEquals(expectedRevision, session.getMostRecentRevisionNumber());
    }
    assertReopenedArray(databasePath, expectedRevision);
  }

  @ParameterizedTest
  @ValueSource(strings = {"before-successor", "before-submission"})
  void inlineHardeningAfterForegroundFailurePreservesDurableSuccess(final String failingStage) {
    final Path databasePath = createDatabase();
    final AtomicBoolean fired = new AtomicBoolean();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final JsonNodeTrx writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      writer.insertArrayAsFirstChild();
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals(failingStage) && fired.compareAndSet(false, true)) {
          throw new SimulatedCrashException(stage);
        }
      };
      assertThrows(IllegalStateException.class, () -> writer.insertStringValueAsFirstChild("uncommitted"));
      assertTrue(fired.get());
      assertDoesNotThrow(writer::awaitPendingAsyncCommit);
      assertTerminal(writer);
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) session).getCommitFile()));
      assertDoesNotThrow(writer::close);
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
    assertReopenedArray(databasePath);
  }

  @ParameterizedTest
  @ValueSource(strings = {"after-publication", "before-pending-root-cleanup", "before-catalogue-acknowledgement",
      "before-diff-publication", "spill", "close-before", "close-after"})
  void asynchronousPostDurableFailureIsNotReportedAsHardeningFailure(final String failingStage) {
    final int expectedRevision = failingStage.equals("before-diff-publication") ? 2 : 1;
    final Path databasePath = createDatabase(failingStage.equals("before-diff-publication"));
    final AtomicBoolean fired = new AtomicBoolean();
    final CountDownLatch release = new CountDownLatch(1);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      if (expectedRevision == 2) {
        try (final JsonNodeTrx seed = session.beginNodeTrx()) {
          seed.commit();
        }
      }
      final JsonNodeTrx writer = session.beginNodeTrx(1, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT);
      writer.insertArrayAsFirstChild();
      final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
      final Point point = switch (failingStage) {
        case "spill" -> Point.BEFORE_DISCARD_SPILLED_WRITES;
        case "close-before" -> Point.BEFORE_CLOSE;
        default -> Point.AFTER_CLOSE;
      };
      final boolean storageFailure = failingStage.equals("spill") || failingStage.startsWith("close-");
      if (storageFailure) {
        injector.injectAt(point, 1);
      }
      AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
        if (stage.equals("before-harden")) {
          await(release);
        }
        if (!storageFailure && stage.equals(failingStage) && fired.compareAndSet(false, true)) {
          throw new SimulatedCrashException(stage);
        }
      };
      try {
        writer.insertStringValueAsFirstChild("speculative");
      } finally {
        release.countDown();
      }
      assertDoesNotThrow(writer::awaitPendingAsyncCommit);
      if (storageFailure) {
        assertFalse(injector.isArmedFor(point));
      } else {
        assertTrue(fired.get());
      }
      assertTerminal(writer);
      assertTrue(Files.exists(((InternalResourceSession<?, ?>) session).getCommitFile()));
      assertThrows(SirixUsageException.class, writer::close);
      assertDoesNotThrow(((InternalNodeTrx<?>) writer)::abort);
    } finally {
      release.countDown();
    }
    assertReopenedArray(databasePath, expectedRevision);
  }

  @ParameterizedTest
  @CsvSource({"KEEP_OPEN,spill,true", "KEEP_OPEN,spill,false",
      "KEEP_OPEN,marker,true", "KEEP_OPEN,marker,false", "KEEP_OPEN,debug,true", "KEEP_OPEN,debug,false",
      "KEEP_OPEN_ASYNC_COMMIT,spill,true", "KEEP_OPEN_ASYNC_COMMIT,spill,false",
      "KEEP_OPEN_ASYNC_COMMIT,marker,true", "KEEP_OPEN_ASYNC_COMMIT,marker,false",
      "KEEP_OPEN_ASYNC_COMMIT,debug,true", "KEEP_OPEN_ASYNC_COMMIT,debug,false"})
  void postBeaconReportingFailurePreservesDurableSuccess(final AfterCommitState mode, final String site,
      final boolean outOfMemory) throws IOException {
    final Path databasePath = createDatabase();
    final CountDownLatch release = new CountDownLatch(1);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final boolean asynchronous = mode == AfterCommitState.KEEP_OPEN_ASYNC_COMMIT;
      final JsonNodeTrx writer = asynchronous ? session.beginNodeTrx(1, mode) : session.beginNodeTrx();
      writer.insertArrayAsFirstChild();
      final StorageEngineWriter predecessor = writer.getStorageEngineWriter();
      final Logger storageLogger = (Logger) LoggerFactory.getLogger(predecessor.getClass());
      final LoggerContext loggerContext = storageLogger.getLoggerContext();
      final Level originalLevel = storageLogger.getLevel();
      final AtomicBoolean reportingFired = new AtomicBoolean();
      final Throwable diagnosticFailure = outOfMemory
          ? new OutOfMemoryError("post-beacon diagnostic failure")
          : new IllegalStateException("post-beacon diagnostic failure");
      final TurboFilter filter = new TurboFilter() {
        @Override
        public FilterReply decide(final Marker marker, final Logger logger, final Level level, final String format,
            final Object[] arguments, final Throwable cause) {
          final boolean target = site.equals("debug")
              ? Level.DEBUG.equals(level) && format != null && format.startsWith("Commit phase 2 r")
              : Level.ERROR.equals(level);
          if (storageLogger.equals(logger) && target && reportingFired.compareAndSet(false, true)) {
            if (diagnosticFailure instanceof Error error) {
              throw error;
            }
            throw (RuntimeException) diagnosticFailure;
          }
          return FilterReply.NEUTRAL;
        }
      };
      filter.start();
      loggerContext.addTurboFilter(filter);
      storageLogger.setLevel(site.equals("debug") ? Level.DEBUG : Level.ERROR);
      try {
        final FaultInjectingWriter injector = FaultInjectingStorage.lastWriter();
        final Path marker = ((InternalResourceSession<?, ?>) session).getCommitFile();
        if (site.equals("spill")) {
          injector.injectAt(Point.BEFORE_DISCARD_SPILLED_WRITES, 1);
        }
        if (asynchronous) {
          AbstractNodeTrxImpl.asyncCommitTestHook = stage -> {
            if (stage.equals("before-harden")) {
              await(release);
            }
          };
          writer.insertStringValueAsFirstChild("speculative");
        }
        try {
          if (site.equals("marker")) {
            Files.deleteIfExists(marker);
            Files.createDirectory(marker);
            Files.createFile(marker.resolve("blocker"));
          }
        } finally {
          release.countDown();
        }
        if (asynchronous) {
          assertDoesNotThrow(writer::awaitPendingAsyncCommit);
        } else {
          assertSame(writer, writer.commit());
        }
        assertTrue(reportingFired.get(), "the post-beacon reporting failure must actually fire");
        if (site.equals("spill")) {
          assertFalse(injector.isArmedFor(Point.BEFORE_DISCARD_SPILLED_WRITES));
          assertInstanceOf(SimulatedCrashException.class, predecessor.postCommitCleanupFailure());
        } else if (site.equals("marker")) {
          assertInstanceOf(IOException.class, predecessor.postCommitCleanupFailure());
        } else {
          assertSame(diagnosticFailure, predecessor.postCommitCleanupFailure());
        }
        assertEquals(1, session.getMostRecentRevisionNumber());
        assertTerminal(writer);
        assertTrue(Files.exists(marker), "the known durable failure must retain repair responsibility");
        if (site.equals("marker")) {
          Files.delete(marker.resolve("blocker"));
          Files.delete(marker);
        }
        if (asynchronous) {
          assertThrows(SirixUsageException.class, writer::close);
          assertDoesNotThrow(((InternalNodeTrx<?>) writer)::abort);
        } else {
          assertDoesNotThrow(writer::close);
        }
      } finally {
        release.countDown();
        loggerContext.getTurboFilterList().remove(filter);
        filter.stop();
        storageLogger.setLevel(originalLevel);
      }
    }
    assertReopenedArray(databasePath);
  }

  @ParameterizedTest
  @EnumSource(value = AfterCommitState.class, names = {"KEEP_OPEN", "KEEP_OPEN_ASYNC_COMMIT", "KEEP_OPEN_ASYNC_FLUSH"})
  void successfulCommitStillAllowsTheNextRevision(final AfterCommitState mode) {
    final Path databasePath = createDatabase();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx writer = session.beginNodeTrx(1, mode)) {
      writer.insertArrayAsFirstChild();
      writer.insertStringValueAsFirstChild("next");
      writer.commit();
      assertEquals(mode == AfterCommitState.KEEP_OPEN_ASYNC_FLUSH ? 1 : 2,
          session.getMostRecentRevisionNumber());
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      assertEquals(1, writer.getChildCount());
    }
  }

  private static void assertTerminal(final JsonNodeTrx writer) {
    assertThrows(IllegalStateException.class, writer::commit);
    assertThrows(IllegalStateException.class, writer::rollback);
    assertThrows(IllegalStateException.class, () -> writer.revertTo(0));
    assertThrows(IllegalStateException.class, () -> writer.truncateTo(0));
  }

  private static void await(final CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS), "hardening was never released");
    } catch (final InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new AssertionError(failure);
    }
  }

  private static void assertReopenedArray(final Path databasePath) {
    assertReopenedArray(databasePath, 1);
  }

  private static void assertReopenedArray(final Path databasePath, final int expectedRevision) {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(expectedRevision, session.getMostRecentRevisionNumber());
      try (final var reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertTrue(reader.isArray());
        assertEquals(0, reader.getChildCount(), "only the committed empty array may survive");
      }
    }
  }

  private Path createDatabase() {
    return createDatabase(false);
  }

  private Path createDatabase(final boolean storeDiffs) {
    final Path databasePath = workRoot.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).storeDiffs(storeDiffs).build());
    }
    return databasePath;
  }

  private static void deleteRecursive(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (final Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (final IOException ignored) {
          // Best effort: a leftover temporary file must not fail the test.
        }
      });
    }
  }
}
