package io.sirix.crash;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Crash-simulation gate for an EXPLICIT rollback ({@code StorageEngineWriter.truncateTo}): unlike
 * crash recovery — where one uber-beacon slot still matches the truncated-to revision — a rollback
 * truncates AWAY the revision both slots advertise, so the ordering between the file truncation
 * and the dual-beacon downgrade is the whole crash-safety story. Truncating FIRST left a window in
 * which a checksum-valid beacon advertised the truncated-away revision over truncated files: a
 * crash there made recovery dereference truncated offsets ("Truncated revisions record") and the
 * resource was permanently unopenable, silently resurrecting a rolled-back revision being the
 * other possible outcome.
 *
 * <p>The gate records every channel write/force/truncate of a real rollback (three committed
 * revisions, then {@code truncateTo(1)}) through {@link PowerLossRecordingStorage}, materializes a
 * candidate post-power-loss state for every crash instant in the rollback window (with lost and
 * torn in-flight variants, {@link CrashStateMaterializer} semantics), and verifies each state
 * cold:
 *
 * <ol>
 *   <li>the resource MUST open — never brick;</li>
 *   <li>it must sit at either the ORIGINAL revision (3 — rollback not yet effective) or the
 *       TARGET revision (1 — rollback effective): nothing in between, nothing beyond;</li>
 *   <li>every surviving revision must serialize exactly to its golden JSON;</li>
 *   <li>the resource must accept a new commit afterwards.</li>
 * </ol>
 *
 * <p>Before the beacons-first ordering fix this failed at every instant between the truncates and
 * the beacon rewrite with the exact production symptom (unopenable, "Truncated revisions record").
 */
public final class ExplicitRollbackCrashSimulationTest {

  private static final String RESOURCE = "rollback-crash-resource";

  private static final int ORIGINAL_REVISION = 3;

  private static final int TARGET_REVISION = 1;

  /** In-flight sets up to this size are enumerated exhaustively (the rollback window is small). */
  private static final int EXHAUSTIVE_SUBSET_LIMIT = 6;

  private record RollbackCandidate(long crashSeq, Set<Long> appliedInFlight, Map<Long, Integer> torn, String label) {
  }

  @Test
  public void anyCrashInstantDuringExplicitRollbackLeavesOriginalOrTargetRevision() throws Exception {
    final Path workRoot = Files.createTempDirectory("sirix-rollback-crash-");
    try {
      // ---------------- record the rollback under the real I/O stack ----------------
      final PowerLossRecorder recorder = new PowerLossRecorder();
      final Path recordDb = workRoot.resolve("record-db");
      final Map<Integer, String> golden = new TreeMap<>();

      PowerLossRecordingStorage.install(recordDb, recorder);
      try {
        Databases.createJsonDatabase(new DatabaseConfiguration(recordDb));
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(recordDb)) {
          database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                       .storageType(StorageType.FILE_CHANNEL)
                                                       .versioningApproach(VersioningType.FULL)
                                                       .hashKind(HashType.NONE)
                                                       .buildPathSummary(false)
                                                       .storeDiffs(false)
                                                       .build());
          try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
            try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"r1\"]"), JsonNodeTrx.Commit.NO);
              wtx.commit();
            }
            for (final String value : new String[] { "{\"r\":2}", "{\"r\":3}" }) {
              try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(value), JsonNodeTrx.Commit.NO);
                wtx.commit();
              }
            }
            assertTrue(session.getMostRecentRevisionNumber() == ORIGINAL_REVISION,
                "setup must produce " + ORIGINAL_REVISION + " revisions");
            for (int revision = 1; revision <= ORIGINAL_REVISION; revision++) {
              golden.put(revision, serializeRevision(session, revision));
            }

            recorder.mark("preRollback", ORIGINAL_REVISION);
            try (final var storageEngineWriter = session.createStorageEngineWriter()) {
              storageEngineWriter.truncateTo(TARGET_REVISION);
            }
            recorder.mark("rollback", TARGET_REVISION);
          }
        }
      } finally {
        PowerLossRecordingStorage.uninstall();
      }

      final Path templateDb = workRoot.resolve("template-db");
      copyRecursive(recordDb, templateDb);
      Files.deleteIfExists(commitMarkerPath(templateDb));

      final List<PowerLossRecorder.Op> ops = recorder.snapshotOps();
      final long rollbackWindowStart = recorder.snapshotMilestones()
                                               .stream()
                                               .filter(m -> m.name().equals("preRollback"))
                                               .findFirst()
                                               .orElseThrow()
                                               .lastSeqInclusive();

      // ---------------- enumerate crash states across the rollback window ----------------
      final List<RollbackCandidate> candidates = enumerateCandidates(ops, rollbackWindowStart);
      final LinkedHashMap<String, RollbackCandidate> uniqueStates = new LinkedHashMap<>();
      final Map<String, byte[]> dataByKey = new LinkedHashMap<>();
      final Map<String, byte[]> revisionsByKey = new LinkedHashMap<>();
      for (final RollbackCandidate candidate : candidates) {
        final byte[] data = CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.DATA,
            candidate.crashSeq(), candidate.appliedInFlight(), candidate.torn());
        final byte[] revisions = CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.REVISIONS,
            candidate.crashSeq(), candidate.appliedInFlight(), candidate.torn());
        final String key = java.util.Arrays.hashCode(data) + "/" + data.length + "|"
            + java.util.Arrays.hashCode(revisions) + "/" + revisions.length;
        if (uniqueStates.putIfAbsent(key, candidate) == null) {
          dataByKey.put(key, data);
          revisionsByKey.put(key, revisions);
        }
      }

      // ---------------- verify each state cold ----------------
      final List<String> failures = new ArrayList<>();
      final Set<Integer> observedRevisions = new HashSet<>();
      int stateIndex = 0;
      for (final Map.Entry<String, RollbackCandidate> entry : uniqueStates.entrySet()) {
        final Path stateDb = workRoot.resolve("state-" + stateIndex++);
        copyRecursive(templateDb, stateDb);
        Files.write(dataFilePath(stateDb), dataByKey.get(entry.getKey()));
        Files.write(revisionsFilePath(stateDb), revisionsByKey.get(entry.getKey()));

        final String failure = verifyState(stateDb, golden, observedRevisions);
        if (failure != null) {
          failures.add("crash state [" + entry.getValue().label() + "]: " + failure);
        }
        dropPerPathRegistryEntries(stateDb);
        deleteRecursive(stateDb);
      }

      if (!failures.isEmpty()) {
        fail(failures.size() + " of " + uniqueStates.size()
            + " rollback crash state(s) violated the original-or-target contract:\n"
            + String.join("\n---\n", failures));
      }
      // The enumeration must actually have exercised both outcomes, or the gate proves nothing.
      assertTrue(observedRevisions.contains(ORIGINAL_REVISION),
          "no crash state surfaced the pre-rollback revision — enumeration broken? saw " + observedRevisions);
      assertTrue(observedRevisions.contains(TARGET_REVISION),
          "no crash state surfaced the rolled-back revision — enumeration broken? saw " + observedRevisions);
    } finally {
      deleteRecursive(workRoot);
    }
  }

  /**
   * Crash instants from the first rollback-issued op through one past the final op (the fully
   * completed rollback), each with exhaustive lost-in-flight subsets and content-aware torn
   * variants for in-flight writes — the dual DSYNC beacon-slot writes are the interesting ones
   * (covers a torn primary falling back to the secondary, i.e. both slot orderings). The
   * pre-rollback boundary op itself (the final op of the LAST COMMIT, whose loss legitimately
   * reverts to the commit-before-last — that commit was then never acknowledged) belongs to the
   * general power-loss gate, not to this rollback-ordering one.
   */
  private static List<RollbackCandidate> enumerateCandidates(final List<PowerLossRecorder.Op> ops,
      final long rollbackWindowStart) {
    final List<RollbackCandidate> candidates = new ArrayList<>();
    for (long instant = rollbackWindowStart + 1; instant <= ops.size(); instant++) {
      final String prefix = "i" + instant;
      final List<PowerLossRecorder.Op> inFlight = inFlightOps(ops, instant);
      final int k = inFlight.size();
      assertTrue(k <= EXHAUSTIVE_SUBSET_LIMIT,
          "rollback window has " + k + " in-flight ops at instant " + instant
              + " — raise the exhaustive limit or sample: " + inFlight);
      for (int mask = 0; mask < (1 << k); mask++) {
        final Set<Long> applied = new HashSet<>();
        for (int bit = 0; bit < k; bit++) {
          if ((mask & (1 << bit)) != 0) {
            applied.add(inFlight.get(bit).seq);
          }
        }
        candidates.add(new RollbackCandidate(instant, applied, Map.of(), prefix + ":mask" + mask));
      }
      for (final PowerLossRecorder.Op op : inFlight) {
        if (op.kind != PowerLossRecorder.OpKind.WRITE || op.bytes.length < 2) {
          continue;
        }
        final Set<Integer> tornLengths = new LinkedHashSet<>();
        tornLengths.add(op.bytes.length / 2);
        int contentEnd = op.bytes.length;
        while (contentEnd > 0 && op.bytes[contentEnd - 1] == 0) {
          contentEnd--;
        }
        if (contentEnd >= 2) {
          tornLengths.add(contentEnd / 2);
          tornLengths.add(contentEnd - 1);
        }
        tornLengths.removeIf(keep -> keep <= 0 || keep >= op.bytes.length);
        final Set<Long> others = new HashSet<>();
        for (final PowerLossRecorder.Op other : inFlight) {
          if (other.seq != op.seq) {
            others.add(other.seq);
          }
        }
        for (final int keep : tornLengths) {
          candidates.add(new RollbackCandidate(instant, others, Map.of(op.seq, keep),
              prefix + ":torn#" + op.seq + "@" + keep + "+rest"));
          candidates.add(new RollbackCandidate(instant, Set.of(), Map.of(op.seq, keep),
              prefix + ":torn#" + op.seq + "@" + keep + "+only"));
        }
      }
    }
    return candidates;
  }

  /** Content-mutating ops issued at or before {@code crashSeq} but past their file's last completed force. */
  private static List<PowerLossRecorder.Op> inFlightOps(final List<PowerLossRecorder.Op> ops, final long crashSeq) {
    final Map<PowerLossRecorder.TargetFile, Long> lastBarrier =
        new java.util.EnumMap<>(PowerLossRecorder.TargetFile.class);
    for (final PowerLossRecorder.Op op : ops) {
      if (op.seq > crashSeq) {
        break;
      }
      if (op.kind == PowerLossRecorder.OpKind.FORCE) {
        lastBarrier.put(op.file, op.seq);
      }
    }
    final List<PowerLossRecorder.Op> inFlight = new ArrayList<>();
    for (final PowerLossRecorder.Op op : ops) {
      if (op.seq > crashSeq) {
        break;
      }
      if (op.kind == PowerLossRecorder.OpKind.FORCE) {
        continue;
      }
      // Write-through (DSYNC/SYNC) writes that RETURNED (a later op was issued) are durable at
      // return — only the boundary op can still be in flight.
      if (op.kind == PowerLossRecorder.OpKind.WRITE && op.durability != PowerLossRecorder.WriteDurability.NONE
          && op.seq < crashSeq) {
        continue;
      }
      if (op.seq > lastBarrier.getOrDefault(op.file, -1L)) {
        inFlight.add(op);
      }
    }
    return inFlight;
  }

  /** @return a failure description, or {@code null} if the state honors the contract. */
  private static String verifyState(final Path stateDb, final Map<Integer, String> golden,
      final Set<Integer> observedRevisions) {
    Databases.clearGlobalCaches();
    final int mostRecent;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(stateDb);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      mostRecent = session.getMostRecentRevisionNumber();
      observedRevisions.add(mostRecent);
      if (mostRecent != ORIGINAL_REVISION && mostRecent != TARGET_REVISION) {
        return "opened at revision " + mostRecent + " but only " + ORIGINAL_REVISION + " (rollback not effective) or "
            + TARGET_REVISION + " (rollback effective) are permitted";
      }
      for (int revision = 1; revision <= mostRecent; revision++) {
        final String json = serializeRevision(session, revision);
        final String expected = golden.get(revision);
        if (!json.equals(expected)) {
          return "revision " + revision + " read back '" + json + "' but golden is '" + expected + "'";
        }
      }
    } catch (final Throwable t) {
      return "resource UNOPENABLE after rollback crash (the beacon/truncate ordering bug):\n" + stackTrace(t);
    }

    // The resource must accept a writer again — offsets and revision-record slots are reused.
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(stateDb);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"recovered\":true}"), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      final int afterRecovery = session.getMostRecentRevisionNumber();
      if (afterRecovery != mostRecent + 1) {
        return "recovery commit produced revision " + afterRecovery + " instead of " + (mostRecent + 1);
      }
      for (int revision = 1; revision <= mostRecent; revision++) {
        final String json = serializeRevision(session, revision);
        final String expected = golden.get(revision);
        if (!json.equals(expected)) {
          return "after the recovery commit, revision " + revision + " read back '" + json + "' but golden is '"
              + expected + "'";
        }
      }
    } catch (final Throwable t) {
      return "resource rejected a writer after the rollback crash:\n" + stackTrace(t);
    }
    return null;
  }

  private static String serializeRevision(final JsonResourceSession session, final int revision) {
    final StringWriter out = new StringWriter();
    JsonSerializer.newBuilder(session, out, revision).build().call();
    return out.toString();
  }

  private static Path resourceDir(final Path dbPath) {
    return dbPath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(RESOURCE);
  }

  private static Path dataFilePath(final Path dbPath) {
    return resourceDir(dbPath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve("sirix.data");
  }

  private static Path revisionsFilePath(final Path dbPath) {
    return resourceDir(dbPath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve("sirix.revisions");
  }

  private static Path commitMarkerPath(final Path dbPath) {
    return resourceDir(dbPath).resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                              .resolve(".commit");
  }

  /** Drop the per-path repository entries a verified scratch state registered, keeping the JVM tidy. */
  private static void dropPerPathRegistryEntries(final Path stateDb) {
    final Path cacheKey = dataFilePath(stateDb);
    StorageType.CACHE_REPOSITORY.remove(cacheKey);
    StorageType.REVISION_INDEX_REPOSITORY.remove(cacheKey);
  }

  private static String stackTrace(final Throwable t) {
    final StringWriter out = new StringWriter();
    t.printStackTrace(new PrintWriter(out));
    final String full = out.toString();
    return full.length() > 4000 ? full.substring(0, 4000) + "\n  ... (truncated)" : full;
  }

  private static void copyRecursive(final Path source, final Path target) throws IOException {
    try (var paths = Files.walk(source)) {
      for (final Path path : (Iterable<Path>) paths::iterator) {
        final Path destination = target.resolve(source.relativize(path));
        if (Files.isDirectory(path)) {
          Files.createDirectories(destination);
        } else {
          Files.createDirectories(destination.getParent());
          Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
  }

  private static void deleteRecursive(final Path root) {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try (var paths = Files.walk(root)) {
      paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    } catch (final IOException ignored) {
      // Best-effort temp cleanup.
    }
  }
}
