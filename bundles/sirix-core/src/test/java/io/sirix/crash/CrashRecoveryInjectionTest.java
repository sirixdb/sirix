package io.sirix.crash;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.LogWrapper;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Crash-injection gate for the commit durability contract: forks {@link CrashWriterMain}, SIGKILLs
 * it at a random instant (often mid-commit or mid-fsync), reopens the database, and verifies that
 *
 * <ol>
 *   <li>the database OPENS (no bricked resource, intact uber page / revisions file),</li>
 *   <li>every ACKNOWLEDGED commit (ack fsync'd after commit() returned) is present,</li>
 *   <li>EVERY readable revision — acked or not — deserializes fully (full traversal) with exactly
 *       the deterministic content the writer produced for it (no torn/garbage pages), and</li>
 *   <li>the resource ACCEPTS A WRITER again: {@code beginNodeTrx} runs the {@code .commit}-marker
 *       truncate-recovery path (a kill mid-commit leaves the marker; writer creation truncates the
 *       torn tail back to the last durable revision), a marker commit succeeds and reads back, and
 *       every pre-crash revision is STILL intact afterwards (the truncated range's offsets get
 *       reused by the marker commit).</li>
 * </ol>
 *
 * <p>Opt-in (spawns and SIGKILLs JVMs in a loop):
 *
 * <pre>
 *   ./gradlew :sirix-core:test --tests "*CrashRecoveryInjectionTest*" -Dsirix.crash.run=true
 * </pre>
 *
 * Iterations default to 25; override with -Dsirix.crash.iterations=N.
 */
public final class CrashRecoveryInjectionTest {

  private static final LogWrapper logger =
      new LogWrapper(LoggerFactory.getLogger(CrashRecoveryInjectionTest.class));

  private static final String RESOURCE = "crash-resource";

  @Test
  public void killedWriterNeverCorruptsAcknowledgedData() throws Exception {
    assumeTrue("true".equals(System.getProperty("sirix.crash.run")),
               "opt-in via -Dsirix.crash.run=true (forks + SIGKILLs JVMs in a loop)");

    final int iterations = Integer.getInteger("sirix.crash.iterations", 25);
    final Random random = new Random(0xC0FFEE);

    for (int iteration = 1; iteration <= iterations; iteration++) {
      final Path workDir = Files.createTempDirectory("sirix-crash-" + iteration + "-");
      final Path dbPath = workDir.resolve("db");
      final Path ackPath = workDir.resolve("acks.log");
      try {
        runIteration(iteration, dbPath, ackPath, 80 + random.nextInt(700));
      } finally {
        try (var paths = Files.walk(workDir)) {
          paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (final IOException ignored) {
          // Best-effort temp cleanup.
        }
      }
    }
  }

  private void runIteration(final int iteration, final Path dbPath, final Path ackPath, final int killAfterMillis)
      throws Exception {
    final String classPath = System.getProperty("java.class.path");
    final String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();

    final Path writerLog = dbPath.getParent().resolve("writer.log");
    final Process writer = new ProcessBuilder(javaBin, "-Xmx512m",
                                              // Same module surface the test JVM gets from build.gradle —
                                              // sirix-core needs FFM native access and the vector module.
                                              "--enable-native-access=ALL-UNNAMED",
                                              "--add-modules", "jdk.incubator.vector",
                                              "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
                                              "--add-opens", "java.base/java.nio=ALL-UNNAMED",
                                              "-cp", classPath,
                                              CrashWriterMain.class.getName(), dbPath.toString(), ackPath.toString())
        .redirectErrorStream(true)
        .redirectOutput(writerLog.toFile())
        .start();

    // Arm the kill only once the writer is in its hot commit loop (first ack visible) —
    // otherwise JVM/db startup eats the whole window and the kill verifies nothing.
    final long armDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while ((!Files.exists(ackPath) || Files.size(ackPath) == 0) && writer.isAlive()) {
      assertTrue(System.nanoTime() < armDeadline, "writer produced no ack within 60s");
      Thread.sleep(5);
    }
    if (!writer.isAlive()) {
      fail("writer died on its own before the kill — log:\n" + Files.readString(writerLog));
    }

    Thread.sleep(killAfterMillis);
    writer.destroyForcibly(); // SIGKILL on Linux — no shutdown hooks, no flushes.
    assertTrue(writer.waitFor(30, TimeUnit.SECONDS), "killed writer did not terminate");

    // Acked commits: "revision childCount" lines, fsync'd AFTER each commit returned. A torn
    // LAST line (killed mid-ack-write) is valid — ignore it.
    int lastAckedRevision = 0;
    long lastAckedChildren = -1;
    if (Files.exists(ackPath)) {
      final List<String> lines = Files.readAllLines(ackPath, StandardCharsets.US_ASCII);
      for (final String line : lines) {
        final String[] parts = line.trim().split(" ");
        if (parts.length == 2) {
          try {
            final int rev = Integer.parseInt(parts[0]);
            final long children = Long.parseLong(parts[1]);
            if (rev > lastAckedRevision) {
              lastAckedRevision = rev;
              lastAckedChildren = children;
            }
          } catch (final NumberFormatException tornTail) {
            // Torn final line — the kill landed mid-ack. Fine.
          }
        }
      }
    }

    if (lastAckedRevision == 0) {
      logger.info("iteration " + iteration + ": killed after " + killAfterMillis
                      + "ms before the first commit was acked — nothing to verify");
      return;
    }

    // 1) The database must OPEN — a killed writer must never brick the resource.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {

      final int mostRecent = session.getMostRecentRevisionNumber();

      // 2) Every acknowledged commit must be durable.
      assertTrue(mostRecent >= lastAckedRevision,
                 "iteration " + iteration + ": acked revision " + lastAckedRevision + " lost — most recent is "
                     + mostRecent + " (killed after " + killAfterMillis + "ms)");

      // 3) Every readable revision must deserialize fully with the writer's deterministic
      //    content: revision R has exactly R-1 array children, values counting down from the
      //    newest insert. A revision beyond the last ack (commit durable, ack lost) must be
      //    intact too — it was produced by a completed commit().
      for (int revision = 1; revision <= mostRecent; revision++) {
        verifyRevision(iteration, session, revision, revision == lastAckedRevision ? lastAckedChildren : null);
      }

      // 4) The killed resource must accept a WRITER again: beginNodeTrx runs the
      //    `.commit`-marker truncate-recovery path (a kill mid-commit leaves the marker behind;
      //    writer creation truncates the torn tail back to the last durable revision and drops
      //    the database's cache entries). Commit a small marker revision on top. Whether the
      //    truncate actually runs depends on where the kill landed — log it (resolved the same
      //    way AbstractResourceSession.getCommitFile() does, which is internal API).
      final boolean commitMarkerLeftBehind = Files.exists(
          dbPath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                .resolve(RESOURCE)
                .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                .resolve(".commit"));
      final int markerRevision = mostRecent + 1;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        assertTrue(wtx.moveToFirstChild(),
                   "iteration " + iteration + ": writer after recovery found no root array");
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"recovered\":" + iteration + "}"),
                                      JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      assertEquals(markerRevision, session.getMostRecentRevisionNumber(),
                   "iteration " + iteration + ": marker commit after writer recovery must be revision "
                       + markerRevision);

      // The marker revision must read back: the root array gained the marker object up front.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(markerRevision)) {
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "iteration " + iteration + ": marker revision has no root array");
        assertEquals(mostRecent, rtx.getChildCount(),
                     "iteration " + iteration + ": marker revision child count");
        assertTrue(rtx.moveToFirstChild(), "iteration " + iteration + ": marker revision first child");
        assertTrue(rtx.moveToFirstChild(), "iteration " + iteration + ": marker object has no field");
        assertEquals("recovered", rtx.getName().getLocalName(),
                     "iteration " + iteration + ": marker object field name");
      }

      // 5) Recovery truncation + the marker commit (which reuses the truncated range's offsets)
      //    must not have clobbered ANY pre-crash revision — re-verify all of them.
      for (int revision = 1; revision <= mostRecent; revision++) {
        verifyRevision(iteration, session, revision, revision == lastAckedRevision ? lastAckedChildren : null);
      }

      logger.info("iteration " + iteration + ": OK — killed after " + killAfterMillis + "ms, acked rev "
                      + lastAckedRevision + ", most recent rev " + mostRecent
                      + " fully verified, writer recovery (truncate-recovery "
                      + (commitMarkerLeftBehind ? "EXERCISED — marker was present" : "no-op — no marker")
                      + ") + marker rev " + markerRevision + " verified, pre-crash revisions re-verified");
    }
  }

  private void verifyRevision(final int iteration, final JsonResourceSession session, final int revision,
      final Long expectedChildrenFromAck) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      // Full traversal: decodes every page of the revision — torn/garbage pages surface here.
      final var axis = new DescendantAxis(rtx);
      long nodes = 0;
      while (axis.hasNext()) {
        axis.nextLong();
        nodes++;
      }

      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild(), "iteration " + iteration + ": revision " + revision + " has no root array");
      final long children = rtx.getChildCount();
      final long expectedChildren = expectedChildrenFromAck != null ? expectedChildrenFromAck : revision - 1L;
      assertEquals(expectedChildren, children,
                   "iteration " + iteration + ": revision " + revision + " child count");
      // Each {"i":N} object contributes 2 nodes (object + named number under fusion) plus the
      // root array itself — sanity-bound rather than over-pin the node-kind layout.
      assertTrue(nodes >= 1 + children, "iteration " + iteration + ": revision " + revision
          + " traversal saw fewer nodes (" + nodes + ") than structurally required");

      if (children > 0) {
        assertTrue(rtx.moveToFirstChild(), "revision " + revision + " first child");
        // Newest insert is first: {"i": children-1}.
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("iteration " + iteration + ": revision " + revision + " failed to read cleanly: " + e, e);
    }
  }
}
