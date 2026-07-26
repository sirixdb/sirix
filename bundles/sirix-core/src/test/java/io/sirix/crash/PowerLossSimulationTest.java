package io.sirix.crash;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.IOStorage;
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
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * POWER-LOSS simulation gate for the commit durability contract — the complement of
 * {@link CrashRecoveryInjectionTest}: the SIGKILL gate validates WRITE ORDER only (after SIGKILL
 * the OS page cache still holds every unforced write), so reordered or LOST unforced writes are
 * never exercised there. This harness records every channel write and {@code force()} barrier of
 * real commits (bootstrap + two data commits, driven through the normal API with
 * {@link RecordingFileChannel}s injected underneath a real {@code FileChannelWriter}), then for
 * every crash instant materializes candidate post-power-loss states under the contract
 *
 * <ul>
 *   <li>writes covered by a completed {@code force()} on their file are durable,</li>
 *   <li>writes after the last completed force may be lost, applied, or torn (prefix only), in any
 *       combination and any order,</li>
 * </ul>
 *
 * and verifies each state cold ({@code Databases.clearGlobalCaches()} + fresh scratch copy):
 *
 * <ol>
 *   <li>if a commit was ACKNOWLEDGED (the API call returned) before the crash instant, the
 *       resource MUST open at a revision ≥ the acked one;</li>
 *   <li>every readable revision must serialize EXACTLY to its golden JSON (no wrong-data
 *       reads);</li>
 *   <li>a revision beyond the acked one may appear only if it is the in-flight commit's (durable
 *       but unacknowledged — documented as correct in docs/DISK_FORMAT.md);</li>
 *   <li>failures must be clean {@code Sirix(IO)Exception}s — never BufferUnderflow / IOOBE / NPE
 *       class unchecked exceptions;</li>
 *   <li>the resource must ACCEPT A WRITER again (the {@code .commit}-marker truncate-recovery
 *       path runs against the materialized state), the recovery commit must succeed, and all
 *       pre-crash revisions must still be intact afterwards.</li>
 * </ol>
 *
 * <p>A seeded-corruption SELF-TEST validates that the verifier actually detects lost acked
 * commits, unopenable acked state and wrong data — a green gate is only meaningful with a
 * demonstrably sharp oracle. A separate metadata-split experiment additionally caps the data
 * file's length at the last {@code force(true)} (stricter than POSIX {@code fdatasync}, which
 * must persist size metadata needed to retrieve data) and reports — without failing the gate on —
 * dirty exceptions that model would produce.
 *
 * <p>Opt-in like the SIGKILL gate:
 *
 * <pre>
 *   ./gradlew :sirix-core:test --tests "*PowerLossSimulationTest*" -Dsirix.crash.run=true
 * </pre>
 *
 * State count is bounded by -Dsirix.crash.powerloss.maxStates (default 320); subset choices are
 * seeded deterministically.
 */
public final class PowerLossSimulationTest {

  private static final String RESOURCE = "powerloss-resource";

  private static final int MAX_STATES = Integer.getInteger("sirix.crash.powerloss.maxStates", 320);

  /** In-flight sets up to this size are enumerated exhaustively; larger ones are sampled. */
  private static final int EXHAUSTIVE_SUBSET_LIMIT = 5;

  private static final int RANDOM_SUBSETS_PER_INSTANT = 20;

  private record Scenario(Path workRoot, Path templateDb, List<PowerLossRecorder.Op> ops,
      List<PowerLossRecorder.Milestone> milestones, Map<Integer, String> golden) {
  }

  private record CrashCandidate(long crashSeq, Set<Long> appliedInFlight, Map<Long, Integer> torn, String label) {
  }

  private record UniqueState(CrashCandidate candidate, byte[] data, byte[] revisions, int ackedRev,
      int maxAttemptedRev, boolean commitMarkerPresent) {
  }

  private record Verdict(String code, boolean failure, String detail) {
    static Verdict pass(final String code) {
      return new Verdict(code, false, "");
    }

    static Verdict fail(final String code, final String detail) {
      return new Verdict(code, true, detail);
    }
  }

  private record Classification(boolean dirty, boolean sirix) {
  }

  private record PipelineResult(Map<String, Integer> verdictCounts, List<String> failures,
      List<String> failureCodes, int statesVerified) {
  }

  // =====================================================================================
  // 1) MAIN GATE: documented force() semantics — any violation fails the test.
  // =====================================================================================

  @Test
  public void anyPowerLossInstantPreservesAcknowledgedCommits() throws Exception {
    assumeTrue("true".equals(System.getProperty("sirix.crash.run")),
               "opt-in via -Dsirix.crash.run=true (materializes and verifies hundreds of crash states)");

    final Scenario scenario = recordScenario();
    try {
      final PipelineResult result = runPipeline(scenario, /* metadataSplit= */ false);
      if (!result.failures().isEmpty()) {
        fail(result.failures().size() + " power-loss state(s) violated the durability contract:\n"
            + String.join("\n---\n", result.failures()));
      }
    } finally {
      deleteRecursive(scenario.workRoot());
    }
  }

  // =====================================================================================
  // 2) METADATA-SPLIT EXPERIMENT: force(false) persists data blocks but NOT the file-size
  //    extension. STRICTER than POSIX fdatasync (which must persist metadata required to
  //    retrieve the data), so dirty/unopenable findings are reported as hardening
  //    observations; only acked-durability and data-integrity violations fail.
  // =====================================================================================

  @Test
  public void metadataSplitLengthLossNeverLosesAckedDataExperiment() throws Exception {
    assumeTrue("true".equals(System.getProperty("sirix.crash.run")),
               "opt-in via -Dsirix.crash.run=true");

    final Scenario scenario = recordScenario();
    try {
      final PipelineResult result = runPipeline(scenario, /* metadataSplit= */ true);
      final Set<String> hardCodes = Set.of("FAIL_LOST_ACKED_COMMIT", "FAIL_PHANTOM_REVISION", "FAIL_WRONG_DATA",
          "FAIL_WRONG_DATA_AFTER_RECOVERY", "FAIL_RECOVERY_COMMIT_REVISION", "FAIL_RECOVERY_COMMIT_CONTENT",
          "FAIL_ACKED_BUT_UNOPENABLE", "FAIL_HARNESS");
      final List<String> hardFailures = new ArrayList<>();
      for (int i = 0; i < result.failureCodes().size(); i++) {
        if (hardCodes.contains(result.failureCodes().get(i))) {
          hardFailures.add(result.failures().get(i));
        } else {
          System.out.println("SOFT FINDING (stricter-than-POSIX metadata-split model only):\n"
              + result.failures().get(i));
        }
      }
      if (!hardFailures.isEmpty()) {
        fail(hardFailures.size() + " metadata-split state(s) lost acked data or read wrong data:\n"
            + String.join("\n---\n", hardFailures));
      }
    } finally {
      deleteRecursive(scenario.workRoot());
    }
  }

  // =====================================================================================
  // 3) ORACLE SELF-TEST: seeded corruptions of a fully-durable state MUST be flagged.
  //    A green gate is only trustworthy if the verifier demonstrably detects violations.
  // =====================================================================================

  @Test
  public void verifierSelfTestDetectsSeededCorruption() throws Exception {
    assumeTrue("true".equals(System.getProperty("sirix.crash.run")),
               "opt-in via -Dsirix.crash.run=true");

    final Scenario scenario = recordScenario();
    try {
      final List<PowerLossRecorder.Op> ops = scenario.ops();
      // One PAST the final op: the run now ends on the primary-beacon write-through WRITE (no
      // trailing force), and the materializer treats the op exactly AT the crash boundary as
      // possibly-unreturned — pristine means every op returned.
      final long pastEnd = ops.get(ops.size() - 1).seq + 1;
      final int finalRev = scenario.milestones().getLast().acknowledgedRevision();
      final byte[] fullData =
          CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.DATA, pastEnd, Set.of(), Map.of());
      final byte[] fullRevisions =
          CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.REVISIONS, pastEnd, Set.of(), Map.of());
      final Path scratch = Files.createDirectories(scenario.workRoot().resolve("selftest"));

      // (a) sanity: the untouched fully-durable state passes.
      {
        final Path stateDir = scratch.resolve("clean");
        writeStateDir(scenario.templateDb(), stateDir, fullData, fullRevisions, false);
        final Verdict verdict = verifyState(stateDir, finalRev, finalRev, scenario.golden());
        assertTrue(!verdict.failure(), "self-test: pristine durable state must pass, got " + verdict);
      }

      // (b) zeroed revisions-file record of the last acked revision => acked state unopenable.
      {
        final byte[] corrupt = fullRevisions.clone();
        final int offset = (int) IOStorage.revisionsFileOffset(finalRev);
        java.util.Arrays.fill(corrupt, offset, offset + IOStorage.REVISIONS_FILE_RECORD_SIZE, (byte) 0);
        final Path stateDir = scratch.resolve("zeroed-revisions-record");
        writeStateDir(scenario.templateDb(), stateDir, fullData, corrupt, false);
        final Verdict verdict = verifyState(stateDir, finalRev, finalRev, scenario.golden());
        assertTrue(verdict.failure(),
                   "self-test: zeroed revisions record for acked revision must be flagged, got " + verdict);
        System.out.println("self-test (b) flagged as expected: " + verdict.code());
      }

      // (c) bit-rot across the last commit's tail pages => acked revision unreadable.
      {
        final byte[] corrupt = fullData.clone();
        for (int i = corrupt.length - 1024; i < corrupt.length; i += 64) {
          corrupt[i] ^= 0x5A;
        }
        final Path stateDir = scratch.resolve("bitrot-last-commit-tail");
        writeStateDir(scenario.templateDb(), stateDir, corrupt, fullRevisions, false);
        final Verdict verdict = verifyState(stateDir, finalRev, finalRev, scenario.golden());
        assertTrue(verdict.failure(), "self-test: bit-rotted acked pages must be flagged, got " + verdict);
        System.out.println("self-test (c) flagged as expected: " + verdict.code());
      }

      // (d) lost-acked detection: a state durable only up to commit1 verified as if commit2 was acked.
      {
        final long commit1Seq = scenario.milestones().get(1).lastSeqInclusive();
        final byte[] data = CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.DATA, commit1Seq,
            Set.of(), Map.of());
        final byte[] revisions = CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.REVISIONS,
            commit1Seq, Set.of(), Map.of());
        final Path stateDir = scratch.resolve("lost-acked");
        writeStateDir(scenario.templateDb(), stateDir, data, revisions, false);
        final Verdict verdict = verifyState(stateDir, finalRev, finalRev, scenario.golden());
        assertTrue(verdict.failure() && verdict.code().equals("FAIL_LOST_ACKED_COMMIT"),
                   "self-test: missing acked revision must be flagged as FAIL_LOST_ACKED_COMMIT, got " + verdict);
        System.out.println("self-test (d) flagged as expected: " + verdict.code());
      }

      // (e) wrong-data detection: pristine state checked against a falsified golden.
      {
        final Map<Integer, String> falsifiedGolden = new HashMap<>(scenario.golden());
        falsifiedGolden.put(finalRev, "[\"not-what-was-committed\"]");
        final Path stateDir = scratch.resolve("wrong-data-oracle");
        writeStateDir(scenario.templateDb(), stateDir, fullData, fullRevisions, false);
        final Verdict verdict = verifyState(stateDir, finalRev, finalRev, falsifiedGolden);
        assertTrue(verdict.failure() && verdict.code().equals("FAIL_WRONG_DATA"),
                   "self-test: golden mismatch must be flagged as FAIL_WRONG_DATA, got " + verdict);
        System.out.println("self-test (e) flagged as expected: " + verdict.code());
      }
    } finally {
      deleteRecursive(scenario.workRoot());
    }
  }

  // =====================================================================================
  // record phase
  // =====================================================================================

  private Scenario recordScenario() throws IOException {
    final Path workRoot = Files.createTempDirectory("sirix-powerloss-");
    final Path recordDb = workRoot.resolve("record-db");
    final Path templateDb = workRoot.resolve("template-db");

    final PowerLossRecorder recorder = new PowerLossRecorder();
    final Map<Integer, String> golden = new TreeMap<>();

    PowerLossRecordingStorage.install(recordDb, recorder);
    try {
      Databases.createJsonDatabase(new DatabaseConfiguration(recordDb));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(recordDb)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .hashKind(HashType.ROLLING)
                                                     .buildPathSummary(true)
                                                     .storeChildCount(true)
                                                     .storeDiffs(false)
                                                     .build());
        try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
          recorder.mark("bootstrap", session.getMostRecentRevisionNumber());
          captureGolden(session, golden);

          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader("[{\"i\":0},{\"deep\":{\"a\":[1,2,3]}},\"alpha\"]"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          recorder.mark("commit1", session.getMostRecentRevisionNumber());
          captureGolden(session, golden);

          // The interesting commit: previous durable state exists, beacons get REWRITTEN.
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild(); // the root array
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"i\":1}"), JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
          recorder.mark("commit2", session.getMostRecentRevisionNumber());
          captureGolden(session, golden);
        }
      }
    } finally {
      PowerLossRecordingStorage.uninstall();
    }

    copyRecursive(recordDb, templateDb);
    Files.deleteIfExists(commitMarkerPath(templateDb));

    final List<PowerLossRecorder.Op> ops = recorder.snapshotOps();
    final List<PowerLossRecorder.Milestone> milestones = recorder.snapshotMilestones();

    System.out.println("=== recorded op stream (" + ops.size() + " ops) ===");
    for (final PowerLossRecorder.Op op : ops) {
      System.out.println("  " + op);
    }
    System.out.println("=== milestones (API call returned => revision acknowledged) ===");
    for (final PowerLossRecorder.Milestone m : milestones) {
      System.out.println(
          "  " + m.name() + ": acked revision " + m.acknowledgedRevision() + " at seq " + m.lastSeqInclusive());
    }
    System.out.println("=== golden revisions ===");
    golden.forEach((rev, json) -> System.out.println("  rev " + rev + ": " + json));

    return new Scenario(workRoot, templateDb, ops, milestones, golden);
  }

  private static void captureGolden(final JsonResourceSession session, final Map<Integer, String> golden) {
    final int mostRecent = session.getMostRecentRevisionNumber();
    for (int revision = 1; revision <= mostRecent; revision++) {
      golden.computeIfAbsent(revision, rev -> serializeRevision(session, rev));
    }
  }

  private static String serializeRevision(final JsonResourceSession session, final int revision) {
    final StringWriter out = new StringWriter();
    JsonSerializer.newBuilder(session, out, revision).build().call();
    return out.toString();
  }

  // =====================================================================================
  // pipeline: enumerate -> materialize -> dedupe -> verify
  // =====================================================================================

  private PipelineResult runPipeline(final Scenario scenario, final boolean metadataSplit) throws IOException {
    final List<PowerLossRecorder.Op> ops = scenario.ops();
    final List<PowerLossRecorder.Milestone> milestones = scenario.milestones();
    final Path scratchRoot =
        Files.createDirectories(scenario.workRoot().resolve(metadataSplit ? "scratch-split" : "scratch"));
    final Path keptFailuresRoot =
        scenario.workRoot().resolveSibling(scenario.workRoot().getFileName() + "-failures");

    final List<CrashCandidate> candidates = enumerateCandidates(ops);
    final LinkedHashMap<String, UniqueState> unique = new LinkedHashMap<>();
    for (final CrashCandidate candidate : candidates) {
      final byte[] data = CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.DATA,
          candidate.crashSeq(), candidate.appliedInFlight(), candidate.torn(), metadataSplit);
      final byte[] revisions = CrashStateMaterializer.materialize(ops, PowerLossRecorder.TargetFile.REVISIONS,
          candidate.crashSeq(), candidate.appliedInFlight(), candidate.torn(), metadataSplit);
      final PowerLossRecorder.Milestone acked = effectiveAckedMilestone(milestones, ops, candidate);
      final PowerLossRecorder.Milestone inFlight = firstMilestoneAfter(milestones, candidate.crashSeq());
      final int ackedRev = acked == null ? -1 : acked.acknowledgedRevision();
      final int maxAttemptedRev =
          inFlight != null ? inFlight.acknowledgedRevision() : milestones.getLast().acknowledgedRevision();
      final boolean marker = inFlight != null;
      final String key =
          sha256(data) + '|' + sha256(revisions) + '|' + ackedRev + '|' + maxAttemptedRev + '|' + marker;
      unique.putIfAbsent(key, new UniqueState(candidate, data, revisions, ackedRev, maxAttemptedRev, marker));
    }

    final List<UniqueState> selected = capStratifiedByInstant(new ArrayList<>(unique.values()));
    System.out.printf("=== [%s] states: %d raw candidates -> %d unique -> %d verified (cap %d) ===%n",
                      metadataSplit ? "metadata-split" : "force-contract", candidates.size(), unique.size(),
                      selected.size(), MAX_STATES);

    final Map<String, Integer> verdictCounts = new TreeMap<>();
    final List<String> failures = new ArrayList<>();
    final List<String> failureCodes = new ArrayList<>();
    final List<String> stateLines = new ArrayList<>();
    int index = 0;
    for (final UniqueState state : selected) {
      final Path stateDir = scratchRoot.resolve("state-" + index);
      writeStateDir(scenario.templateDb(), stateDir, state.data(), state.revisions(), state.commitMarkerPresent());

      Verdict verdict;
      try {
        verdict = verifyState(stateDir, state.ackedRev(), state.maxAttemptedRev(), scenario.golden());
      } catch (final Throwable unexpected) {
        verdict = Verdict.fail("FAIL_HARNESS", stackTrace(unexpected));
      }
      verdictCounts.merge(verdict.code(), 1, Integer::sum);
      stateLines.add(String.format("  %-34s acked=%2d attempted=%d marker=%-5s data=%6dB rev=%5dB -> %s",
                                   state.candidate().label(), state.ackedRev(), state.maxAttemptedRev(),
                                   state.commitMarkerPresent(), state.data().length, state.revisions().length,
                                   verdict.code()));

      if (verdict.failure()) {
        final Path kept = keptFailuresRoot.resolve((metadataSplit ? "split-" : "") + stateDir.getFileName());
        Files.createDirectories(keptFailuresRoot);
        copyRecursive(stateDir, kept);
        failures.add(describeFailure(state, verdict, ops, kept));
        failureCodes.add(verdict.code());
      }
      dropPerPathRegistryEntries(stateDir);
      deleteRecursive(stateDir);
      index++;
    }

    System.out.println("=== [" + (metadataSplit ? "metadata-split" : "force-contract") + "] per-state verdicts ===");
    stateLines.forEach(System.out::println);
    System.out.println("=== [" + (metadataSplit ? "metadata-split" : "force-contract") + "] verdict histogram ===");
    verdictCounts.forEach((code, count) -> System.out.printf("  %-44s %d%n", code, count));
    if (!failures.isEmpty()) {
      System.out.println("=== [" + (metadataSplit ? "metadata-split" : "force-contract") + "] FAILURES ("
          + failures.size() + ") ===");
      failures.forEach(System.out::println);
    }
    return new PipelineResult(verdictCounts, failures, failureCodes, selected.size());
  }

  // =====================================================================================
  // candidate enumeration
  // =====================================================================================

  private static List<CrashCandidate> enumerateCandidates(final List<PowerLossRecorder.Op> ops) {
    final List<CrashCandidate> candidates = new ArrayList<>();
    for (long instant = -1; instant < ops.size(); instant++) {
      final List<PowerLossRecorder.Op> inFlight = inFlightOps(ops, instant);
      final String prefix = "i" + instant;
      if (inFlight.isEmpty()) {
        candidates.add(new CrashCandidate(instant, Set.of(), Map.of(), prefix + ":quiesced"));
        continue;
      }

      final int k = inFlight.size();
      if (k <= EXHAUSTIVE_SUBSET_LIMIT) {
        for (int mask = 0; mask < (1 << k); mask++) {
          final Set<Long> applied = new HashSet<>();
          for (int bit = 0; bit < k; bit++) {
            if ((mask & (1 << bit)) != 0) {
              applied.add(inFlight.get(bit).seq);
            }
          }
          candidates.add(new CrashCandidate(instant, applied, Map.of(), prefix + ":mask" + mask));
        }
      } else {
        candidates.add(new CrashCandidate(instant, Set.of(), Map.of(), prefix + ":none"));
        final Set<Long> all = new HashSet<>();
        inFlight.forEach(op -> all.add(op.seq));
        candidates.add(new CrashCandidate(instant, all, Map.of(), prefix + ":all"));
        final Random random = new Random(0xC0FFEEL ^ (instant * 1009L));
        for (int sample = 0; sample < RANDOM_SUBSETS_PER_INSTANT; sample++) {
          final Set<Long> applied = new HashSet<>();
          for (final PowerLossRecorder.Op op : inFlight) {
            if (random.nextBoolean()) {
              applied.add(op.seq);
            }
          }
          candidates.add(new CrashCandidate(instant, applied, Map.of(), prefix + ":seed" + sample));
        }
      }

      // Torn-write variants: one in-flight write applied only as a prefix — once with every
      // other in-flight op applied, once with everything else lost. Tear points are both
      // sector-granular (512B, half, all-but-512B) and CONTENT-AWARE: an uber-beacon slot is
      // ~22 bytes of [len][payload][xxh3] followed by ~4 KiB of zero padding, so sector-granular
      // tears of it are bit-identical to the full write (the dedupe proved this) — the tears
      // that actually corrupt the slot must cut inside the meaningful prefix.
      for (final PowerLossRecorder.Op op : inFlight) {
        if (op.kind != PowerLossRecorder.OpKind.WRITE || op.bytes.length < 2) {
          continue;
        }
        final Set<Integer> tornLengths = new LinkedHashSet<>();
        tornLengths.add(op.bytes.length / 2);
        if (op.bytes.length >= 1024) {
          tornLengths.add(512);
          tornLengths.add(op.bytes.length - 512);
        }
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
          candidates.add(new CrashCandidate(instant, others, Map.of(op.seq, keep),
              prefix + ":torn#" + op.seq + "@" + keep + "+rest"));
          candidates.add(new CrashCandidate(instant, Set.of(), Map.of(op.seq, keep),
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
      if (op.seq > lastBarrier.getOrDefault(op.file, -1L)) {
        inFlight.add(op);
      }
    }
    return inFlight;
  }

  /**
   * The acked milestone for a crash state, with crash-boundary correction: with the
   * write-through commit protocol a milestone's FINAL op is the primary-beacon WRITE, and the
   * API returns only after that write completes — so when the boundary write was dropped or
   * torn in this state, the caller never observed the acknowledgement and the previous
   * milestone is the acked one. (Boundary FORCE ops — the legacy ack — keep the historical
   * issued-equals-effective treatment; the not-yet-effective case is covered by the previous
   * crash instant's enumeration.)
   */
  private static PowerLossRecorder.Milestone effectiveAckedMilestone(
      final List<PowerLossRecorder.Milestone> milestones, final List<PowerLossRecorder.Op> ops,
      final CrashCandidate candidate) {
    PowerLossRecorder.Milestone acked = lastMilestoneAtOrBefore(milestones, candidate.crashSeq());
    if (acked != null && acked.lastSeqInclusive() == candidate.crashSeq()
        && candidate.crashSeq() >= 0 && candidate.crashSeq() < ops.size()) {
      final PowerLossRecorder.Op boundary = ops.get((int) candidate.crashSeq());
      if (boundary.kind == PowerLossRecorder.OpKind.WRITE) {
        final boolean fullyApplied = !candidate.torn().containsKey(boundary.seq)
            && candidate.appliedInFlight().contains(boundary.seq);
        if (!fullyApplied) {
          acked = lastMilestoneAtOrBefore(milestones, candidate.crashSeq() - 1);
        }
      }
    }
    return acked;
  }

  private static PowerLossRecorder.Milestone lastMilestoneAtOrBefore(
      final List<PowerLossRecorder.Milestone> milestones, final long crashSeq) {
    PowerLossRecorder.Milestone result = null;
    for (final PowerLossRecorder.Milestone milestone : milestones) {
      if (milestone.lastSeqInclusive() <= crashSeq) {
        result = milestone;
      }
    }
    return result;
  }

  private static PowerLossRecorder.Milestone firstMilestoneAfter(final List<PowerLossRecorder.Milestone> milestones,
      final long crashSeq) {
    for (final PowerLossRecorder.Milestone milestone : milestones) {
      if (milestone.lastSeqInclusive() > crashSeq) {
        return milestone;
      }
    }
    return null;
  }

  /** Keeps at most {@link #MAX_STATES} states, round-robin across crash instants so every region keeps coverage. */
  private static List<UniqueState> capStratifiedByInstant(final List<UniqueState> states) {
    if (states.size() <= MAX_STATES) {
      return states;
    }
    final Map<Long, Deque<UniqueState>> byInstant = new TreeMap<>();
    for (final UniqueState state : states) {
      byInstant.computeIfAbsent(state.candidate().crashSeq(), k -> new ArrayDeque<>()).add(state);
    }
    final List<UniqueState> selected = new ArrayList<>(MAX_STATES);
    while (selected.size() < MAX_STATES) {
      boolean tookAny = false;
      for (final Deque<UniqueState> bucket : byInstant.values()) {
        if (!bucket.isEmpty() && selected.size() < MAX_STATES) {
          selected.add(bucket.poll());
          tookAny = true;
        }
      }
      if (!tookAny) {
        break;
      }
    }
    return selected;
  }

  // =====================================================================================
  // state materialization on disk
  // =====================================================================================

  private static void writeStateDir(final Path templateDb, final Path stateDir, final byte[] data,
      final byte[] revisions, final boolean commitMarkerPresent) throws IOException {
    copyRecursive(templateDb, stateDir);
    Files.write(dataFilePath(stateDir), data);
    Files.write(revisionsFilePath(stateDir), revisions);
    final Path marker = commitMarkerPath(stateDir);
    if (commitMarkerPresent) {
      Files.createDirectories(marker.getParent());
      if (!Files.exists(marker)) {
        Files.createFile(marker);
      }
    } else {
      Files.deleteIfExists(marker);
    }
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

  // =====================================================================================
  // verification
  // =====================================================================================

  private Verdict verifyState(final Path stateDb, final int ackedRev, final int maxAttemptedRev,
      final Map<Integer, String> golden) {
    // ---- read-only, cold ----
    Databases.clearGlobalCaches();
    final int mostRecent;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(stateDb);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      mostRecent = session.getMostRecentRevisionNumber();
      if (mostRecent < ackedRev) {
        return Verdict.fail("FAIL_LOST_ACKED_COMMIT",
            "opened at revision " + mostRecent + " but revision " + ackedRev + " was acknowledged");
      }
      if (mostRecent > maxAttemptedRev) {
        return Verdict.fail("FAIL_PHANTOM_REVISION",
            "opened at revision " + mostRecent + " but only " + maxAttemptedRev + " was ever attempted");
      }
      for (int revision = 1; revision <= mostRecent; revision++) {
        final String json = serializeRevision(session, revision);
        final String expected = golden.get(revision);
        if (!json.equals(expected)) {
          return Verdict.fail("FAIL_WRONG_DATA",
              "revision " + revision + " read back '" + json + "' but golden is '" + expected + "'");
        }
      }
    } catch (final Throwable t) {
      final Classification cls = classify(t);
      if (cls.dirty()) {
        return Verdict.fail("FAIL_DIRTY_EXCEPTION_READ", stackTrace(t));
      }
      if (ackedRev >= 0) {
        return Verdict.fail("FAIL_ACKED_BUT_UNOPENABLE", stackTrace(t));
      }
      if (!cls.sirix()) {
        return Verdict.fail("FAIL_UNCLEAN_NON_SIRIX_READ", stackTrace(t));
      }
      return Verdict.pass("PASS_CLEAN_REJECT_NOTHING_ACKED");
    }

    // ---- writer recovery, cold again ----
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(stateDb);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int beforeRecovery = session.getMostRecentRevisionNumber();
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) { // runs marker truncate-recovery
        wtx.moveToDocumentRoot();
        if (wtx.moveToFirstChild()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"recovered\":1}"), JsonNodeTrx.Commit.NO);
        } else {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"), JsonNodeTrx.Commit.NO);
        }
        wtx.commit();
      }
      final int afterRecovery = session.getMostRecentRevisionNumber();
      if (afterRecovery != beforeRecovery + 1) {
        return Verdict.fail("FAIL_RECOVERY_COMMIT_REVISION",
            "recovery commit produced revision " + afterRecovery + " instead of " + (beforeRecovery + 1));
      }
      // Recovery truncation + offset reuse by the recovery commit must not clobber ANY pre-crash
      // revision.
      for (int revision = 1; revision <= beforeRecovery; revision++) {
        final String json = serializeRevision(session, revision);
        final String expected = golden.get(revision);
        if (!json.equals(expected)) {
          return Verdict.fail("FAIL_WRONG_DATA_AFTER_RECOVERY",
              "after recovery commit, revision " + revision + " read back '" + json + "' but golden is '" + expected
                  + "'");
        }
      }
      final String recoveredJson = serializeRevision(session, afterRecovery);
      if (beforeRecovery >= 1 && !recoveredJson.contains("recovered")) {
        return Verdict.fail("FAIL_RECOVERY_COMMIT_CONTENT",
            "recovery revision " + afterRecovery + " does not contain the marker object: '" + recoveredJson + "'");
      }
    } catch (final Throwable t) {
      final Classification cls = classify(t);
      if (cls.dirty()) {
        return Verdict.fail("FAIL_DIRTY_EXCEPTION_WRITER", stackTrace(t));
      }
      if (ackedRev >= 0) {
        return Verdict.fail("FAIL_WRITER_REJECTED", stackTrace(t));
      }
      if (!cls.sirix()) {
        return Verdict.fail("FAIL_UNCLEAN_NON_SIRIX_WRITER", stackTrace(t));
      }
      return Verdict.pass("PASS_WRITER_CLEAN_REJECT_NOTHING_ACKED");
    }

    return Verdict.pass(ackedRev >= 0 ? "PASS" : "PASS_NOTHING_ACKED");
  }

  /**
   * Walks the throwable chain (causes + suppressed). "Dirty" = unchecked low-level exception
   * classes that must never escape from reading a corrupt file; "sirix" = the documented clean
   * failure types.
   */
  private static Classification classify(final Throwable root) {
    boolean dirty = false;
    boolean sirix = false;
    final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    final Deque<Throwable> queue = new ArrayDeque<>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final Throwable t = queue.poll();
      if (t == null || !seen.add(t)) {
        continue;
      }
      if (t instanceof java.nio.BufferUnderflowException || t instanceof java.nio.BufferOverflowException
          || t instanceof IndexOutOfBoundsException || t instanceof NegativeArraySizeException
          || t instanceof NullPointerException || t instanceof ClassCastException
          || t instanceof ArithmeticException || t instanceof ArrayStoreException
          || t instanceof StackOverflowError || t instanceof OutOfMemoryError || t instanceof AssertionError) {
        dirty = true;
      }
      if (t instanceof io.sirix.exception.SirixException || t instanceof io.sirix.exception.SirixRuntimeException) {
        sirix = true;
      }
      if (t.getCause() != null) {
        queue.add(t.getCause());
      }
      Collections.addAll(queue, t.getSuppressed());
    }
    return new Classification(dirty, sirix);
  }

  // =====================================================================================
  // reporting helpers
  // =====================================================================================

  private static String describeFailure(final UniqueState state, final Verdict verdict,
      final List<PowerLossRecorder.Op> ops, final Path keptDir) {
    final StringWriter out = new StringWriter();
    final PrintWriter print = new PrintWriter(out);
    final CrashCandidate candidate = state.candidate();
    print.println("FAILURE " + verdict.code() + " at crash instant seq=" + candidate.crashSeq() + " ["
        + candidate.label() + "]");
    if (candidate.crashSeq() >= 0 && candidate.crashSeq() < ops.size()) {
      print.println("  last issued op: " + ops.get((int) candidate.crashSeq()));
    } else {
      print.println("  last issued op: <none — before the first channel op>");
    }
    print.println("  applied in-flight seqs: " + new java.util.TreeSet<>(candidate.appliedInFlight()));
    print.println("  torn writes (seq -> kept prefix bytes): " + new TreeMap<>(candidate.torn()));
    print.println("  acked revision: " + state.ackedRev() + ", max attempted: " + state.maxAttemptedRev()
        + ", .commit marker: " + state.commitMarkerPresent());
    print.println("  materialized sizes: data=" + state.data().length + "B revisions=" + state.revisions().length
        + "B (state kept at " + keptDir + ")");
    print.println("  detail: " + verdict.detail());
    return out.toString();
  }

  private static String stackTrace(final Throwable t) {
    final StringWriter out = new StringWriter();
    t.printStackTrace(new PrintWriter(out));
    final String full = out.toString();
    return full.length() > 6000 ? full.substring(0, 6000) + "\n  ... (truncated)" : full;
  }

  // =====================================================================================
  // misc utilities
  // =====================================================================================

  private static String sha256(final byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (final java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
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
      paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    } catch (final IOException ignored) {
      // Best-effort temp cleanup.
    }
  }

  /** Drop the per-path repository entries a verified scratch state registered, keeping the JVM tidy. */
  private static void dropPerPathRegistryEntries(final Path stateDb) {
    final Path cacheKey = resourceDir(stateDb).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                              .resolve("sirix.data");
    StorageType.CACHE_REPOSITORY.remove(cacheKey);
    StorageType.REVISION_INDEX_REPOSITORY.remove(cacheKey);
  }
}
