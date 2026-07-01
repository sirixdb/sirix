package io.sirix.fuzz;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.temporal.AllTimeAxis;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Wall-clock-bounded <em>soak</em> fuzz harness: keeps generating and checking
 * randomized workloads against Sirix until a configurable deadline elapses.
 * Unlike the fixed-iteration fuzz tests ({@link JsonRoundTripFuzz},
 * {@link RandomWorkloadTemporalConsistencyFuzz}, {@link PageCorruptionFuzz}),
 * which each run a bounded number of iterations and finish in seconds, this one
 * is meant to run for minutes-to-hours to shake out rare, timing- or
 * state-accumulation-dependent defects.
 *
 * <h2>Opt-in only</h2>
 * The test is gated behind the system property {@code sirix.fuzz.soak=true} so
 * it never runs in normal unit-test / CI passes (a multi-hour test would wreck
 * a CI budget). Run it explicitly, e.g.:
 * <pre>{@code
 *   ./gradlew :sirix-core:test \
 *       --tests io.sirix.fuzz.ContinuousFuzzSoak \
 *       -Dsirix.fuzz.soak=true \
 *       -Dsirix.fuzz.durationMinutes=300 \
 *       -Dsirix.fuzz.seed=0x5127A1
 * }</pre>
 *
 * <h2>Configuration (all optional, via {@code -D} system properties)</h2>
 * <ul>
 *   <li>{@code sirix.fuzz.soak}          — must be {@code true} to enable at all.</li>
 *   <li>{@code sirix.fuzz.durationMinutes} — wall-clock budget; default {@value #DEFAULT_DURATION_MINUTES} (5h).</li>
 *   <li>{@code sirix.fuzz.seed}          — master seed (decimal or {@code 0x}-hex);
 *       default {@value #DEFAULT_SEED_LITERAL}. The seed fully determines the run,
 *       so a failure is reproducible by re-running with the same value.</li>
 *   <li>{@code sirix.fuzz.progressSeconds} — heartbeat interval for the progress line;
 *       default {@value #DEFAULT_PROGRESS_SECONDS}.</li>
 * </ul>
 *
 * <h2>Reproducibility</h2>
 * Every iteration draws a fresh 64-bit iteration seed from the master
 * {@link SplittableRandom}. On failure the harness prints the master seed, the
 * iteration index, the iteration seed and the workload kind, so a single failing
 * iteration can be replayed in isolation by constructing a
 * {@code new SplittableRandom(iterationSeed)}.
 */
@EnabledIfSystemProperty(named = "sirix.fuzz.soak", matches = "true")
final class ContinuousFuzzSoak {

  /** Default budget: five hours. */
  private static final int DEFAULT_DURATION_MINUTES = 300;
  private static final long DEFAULT_SEED = 0x5127A150ACL;
  private static final String DEFAULT_SEED_LITERAL = "0x5127A150AC";
  private static final int DEFAULT_PROGRESS_SECONDS = 60;

  // ---- JSON generator bounds (mirrors JsonRoundTripFuzz) ----
  private static final int MAX_DEPTH = 6;
  private static final int MAX_BRANCH = 7;
  private static final int ASCII_LOW = 0x21;
  private static final int ASCII_HIGH = 0x7E;
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  // ---- temporal-mutation workload bounds ----
  private static final int MUT_INITIAL_SIZE = 8;
  private static final int MUT_MAX_OPS = 40;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Soak: run seeded random workloads until the wall-clock budget is exhausted")
  void soak() throws Exception {
    final long masterSeed = readSeedProperty();
    final long durationMinutes = readLongProperty("sirix.fuzz.durationMinutes", DEFAULT_DURATION_MINUTES);
    final long progressSeconds = readLongProperty("sirix.fuzz.progressSeconds", DEFAULT_PROGRESS_SECONDS);

    final long budgetNanos = TimeUnit.MINUTES.toNanos(durationMinutes);
    final long progressNanos = TimeUnit.SECONDS.toNanos(progressSeconds);
    final long startNanos = System.nanoTime();
    final long deadlineNanos = startNanos + budgetNanos;

    final SplittableRandom master = new SplittableRandom(masterSeed);

    System.out.printf("[soak] start masterSeed=0x%s durationMinutes=%d progressSeconds=%d%n",
                      Long.toHexString(masterSeed), durationMinutes, progressSeconds);

    // Sirix releases memory-mapped data-file descriptors lazily (via Cleaner on GC),
    // so a fast create/close/delete loop can exhaust the process fd limit long before
    // the JVM would otherwise collect them. Periodically nudge GC and briefly yield so
    // native cleaners run and fds stay bounded across a multi-hour run.
    final long gcEveryIterations = readLongProperty("sirix.fuzz.gcEvery", 25);

    long iteration = 0;
    long roundTripRuns = 0;
    long temporalRuns = 0;
    long lastProgressNanos = startNanos;

    while (System.nanoTime() < deadlineNanos) {
      final long iterSeed = master.nextLong();
      // Pick a workload. Round-trip is cheap and broad; temporal is heavier and
      // exercises multi-revision history — weight 2:1 toward round-trip.
      final boolean roundTrip = Math.floorMod(iterSeed, 3L) != 0L;
      final Workload kind = roundTrip ? Workload.ROUND_TRIP : Workload.TEMPORAL;

      try {
        switch (kind) {
          case ROUND_TRIP -> {
            runRoundTrip(iterSeed);
            roundTripRuns++;
          }
          case TEMPORAL -> {
            runTemporalConsistency(iterSeed);
            temporalRuns++;
          }
        }
      } catch (final AssertionError | Exception failure) {
        final long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
        fail("[soak] FAILURE after " + elapsedSec + "s"
                 + " masterSeed=0x" + Long.toHexString(masterSeed)
                 + " iteration=" + iteration
                 + " workload=" + kind
                 + " iterationSeed=0x" + Long.toHexString(iterSeed)
                 + " — reproduce this iteration with new SplittableRandom(0x"
                 + Long.toHexString(iterSeed) + "L). Cause: " + failure,
             failure);
      } finally {
        // Reset on-disk state between iterations so history/size doesn't grow unbounded.
        JsonTestHelper.deleteEverything();
      }

      iteration++;

      if (iteration % gcEveryIterations == 0) {
        System.gc();
        try {
          Thread.sleep(20);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      final long now = System.nanoTime();
      if (now - lastProgressNanos >= progressNanos) {
        final long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(now - startNanos);
        final long remainingSec = Math.max(0, TimeUnit.NANOSECONDS.toSeconds(deadlineNanos - now));
        System.out.printf(
            "[soak] elapsed=%ds remaining=%ds iterations=%d (roundTrip=%d temporal=%d) rate=%.1f/s%n",
            elapsedSec, remainingSec, iteration, roundTripRuns, temporalRuns,
            elapsedSec == 0 ? 0.0 : (double) iteration / elapsedSec);
        lastProgressNanos = now;
      }
    }

    final long totalSec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
    System.out.printf("[soak] DONE elapsed=%ds iterations=%d (roundTrip=%d temporal=%d) — no failures%n",
                      totalSec, iteration, roundTripRuns, temporalRuns);
  }

  private enum Workload { ROUND_TRIP, TEMPORAL }

  // ==================== WORKLOAD 1: JSON round-trip ====================

  /**
   * Generate a random well-formed JSON document, push it through
   * shred → commit → serialize, and assert the streamed token sequence is
   * preserved (the same oracle {@link JsonRoundTripFuzz} uses).
   */
  private static void runRoundTrip(final long iterSeed) throws Exception {
    final SplittableRandom rng = new SplittableRandom(iterSeed);
    final String input = generateJson(rng, 0);

    final List<String> inputTokens = tokenize(input);
    final String output = roundTripThroughSirix(input);
    final List<String> outputTokens = tokenize(output);

    assertEquals(inputTokens, outputTokens,
                 () -> "round-trip token mismatch\n  input  = " + input + "\n  output = " + output);
  }

  private static String roundTripThroughSirix(final String input) throws Exception {
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(input), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final StringWriter out = new StringWriter();
      JsonSerializer.newBuilder(session, out).build().call();
      return out.toString();
    }
  }

  private static List<String> tokenize(final String json) throws Exception {
    final List<String> tokens = new ArrayList<>();
    try (JsonParser parser = JSON_FACTORY.createParser(json)) {
      JsonToken t;
      while ((t = parser.nextToken()) != null) {
        switch (t) {
          case START_OBJECT -> tokens.add("OBJ_START");
          case END_OBJECT -> tokens.add("OBJ_END");
          case START_ARRAY -> tokens.add("ARR_START");
          case END_ARRAY -> tokens.add("ARR_END");
          case FIELD_NAME -> tokens.add("FIELD:" + parser.getCurrentName());
          case VALUE_STRING -> tokens.add("STR:" + parser.getValueAsString());
          case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> tokens.add("NUM:" + parser.getValueAsString());
          case VALUE_TRUE -> tokens.add("BOOL:true");
          case VALUE_FALSE -> tokens.add("BOOL:false");
          case VALUE_NULL -> tokens.add("NULL");
          default -> tokens.add("OTHER:" + t.name());
        }
      }
    }
    return tokens;
  }

  private static String generateJson(final SplittableRandom rng, final int depth) {
    if (depth > 0 && (depth >= MAX_DEPTH || rng.nextInt(0, 4) == 0)) {
      return generateLeaf(rng);
    }
    return rng.nextBoolean() ? generateArray(rng, depth) : generateObject(rng, depth);
  }

  private static String generateArray(final SplittableRandom rng, final int depth) {
    final int size = rng.nextInt(0, MAX_BRANCH + 1);
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < size; i++) {
      if (i > 0) sb.append(',');
      sb.append(generateJson(rng, depth + 1));
    }
    return sb.append(']').toString();
  }

  private static String generateObject(final SplittableRandom rng, final int depth) {
    final int size = rng.nextInt(0, MAX_BRANCH + 1);
    final StringBuilder sb = new StringBuilder("{");
    final HashSet<String> seen = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      String key;
      do {
        key = generateAsciiString(rng, 1, 8);
      } while (!seen.add(key));
      if (i > 0) sb.append(',');
      sb.append('"').append(key).append('"').append(':').append(generateJson(rng, depth + 1));
    }
    return sb.append('}').toString();
  }

  private static String generateLeaf(final SplittableRandom rng) {
    return switch (rng.nextInt(0, 4)) {
      case 0 -> "null";
      case 1 -> rng.nextBoolean() ? "true" : "false";
      case 2 -> Integer.toString(rng.nextInt(-1_000_000, 1_000_001));
      default -> '"' + generateAsciiString(rng, 0, 16) + '"';
    };
  }

  private static String generateAsciiString(final SplittableRandom rng, final int minLen, final int maxLen) {
    final int len = rng.nextInt(minLen, maxLen + 1);
    final StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      int c;
      do {
        c = rng.nextInt(ASCII_LOW, ASCII_HIGH + 1);
      } while (c == '"' || c == '\\');
      sb.append((char) c);
    }
    return sb.toString();
  }

  // ==================== WORKLOAD 2: temporal consistency ====================

  /**
   * Seed a number array, apply a random sequence of insert/update/delete commits
   * against a parallel in-memory model, then verify that every revision — latest
   * and all prior — still reads exactly as its recorded snapshot, plus an
   * {@link AllTimeAxis} revision-count check (the same contract as
   * {@link RandomWorkloadTemporalConsistencyFuzz}, with a per-iteration seed).
   */
  private static void runTemporalConsistency(final long iterSeed) throws Exception {
    final SplittableRandom rng = new SplittableRandom(iterSeed);
    final int operations = rng.nextInt(1, MUT_MAX_OPS + 1);

    final List<List<Integer>> expectedPerRevision = new ArrayList<>();
    final List<Integer> seed = new ArrayList<>(MUT_INITIAL_SIZE);
    for (int i = 0; i < MUT_INITIAL_SIZE; i++) {
      seed.add(i);
    }
    seedArray(seed);
    expectedPerRevision.add(new ArrayList<>(seed));

    final List<Integer> live = new ArrayList<>(seed);
    int currentRevision = 1;

    for (int op = 0; op < operations; op++) {
      final int roll = rng.nextInt(0, 100);
      final OpKind kind;
      if (roll < 50) kind = OpKind.INSERT;
      else if (roll < 80) kind = OpKind.UPDATE;
      else kind = live.isEmpty() ? OpKind.INSERT : OpKind.DELETE;

      final int randomValue = rng.nextInt(-10_000, 10_001);
      final int randomIndex = live.isEmpty() ? 0 : rng.nextInt(0, live.size());

      switch (kind) {
        case INSERT -> applyInsert(randomIndex, randomValue, live);
        case UPDATE -> {
          if (live.isEmpty()) {
            applyInsert(0, randomValue, live);
          } else {
            applyUpdate(randomIndex, randomValue, live);
          }
        }
        case DELETE -> applyDelete(randomIndex, live);
      }
      currentRevision++;
      expectedPerRevision.add(new ArrayList<>(live));
    }

    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int rev = 1; rev <= currentRevision; rev++) {
        final List<Integer> expected = expectedPerRevision.get(rev - 1);
        final List<Integer> observed = readArray(session, rev);
        final int finalRev = rev;
        assertEquals(expected, observed,
                     () -> "revision " + finalRev + " diverges:\n  expected = " + expected
                         + "\n  observed = " + observed);
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(currentRevision)) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        int yielded = 0;
        final var axis = new AllTimeAxis<>(session, rtx);
        while (axis.hasNext()) {
          final var yieldedRtx = axis.next();
          assertNotNull(yieldedRtx);
          yieldedRtx.close();
          yielded++;
        }
        assertEquals(currentRevision, yielded,
                     "AllTimeAxis on the array root yielded " + yielded + " rtxs, expected " + currentRevision);
      }
    }
  }

  private enum OpKind { INSERT, UPDATE, DELETE }

  private static void applyInsert(final int randomIndex, final int value, final List<Integer> live)
      throws Exception {
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      if (live.isEmpty()) {
        wtx.insertNumberValueAsFirstChild(value);
        live.add(0, value);
      } else {
        wtx.moveToFirstChild();
        for (int i = 0; i < randomIndex; i++) {
          wtx.moveToRightSibling();
        }
        wtx.insertNumberValueAsRightSibling(value);
        live.add(randomIndex + 1, value);
      }
      wtx.commit();
    }
  }

  private static void applyUpdate(final int randomIndex, final int value, final List<Integer> live)
      throws Exception {
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      for (int i = 0; i < randomIndex; i++) {
        wtx.moveToRightSibling();
      }
      wtx.setNumberValue(value);
      live.set(randomIndex, value);
      wtx.commit();
    }
  }

  private static void applyDelete(final int randomIndex, final List<Integer> live)
      throws Exception {
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      for (int i = 0; i < randomIndex; i++) {
        wtx.moveToRightSibling();
      }
      wtx.remove();
      live.remove(randomIndex);
      wtx.commit();
    }
  }

  private static void seedArray(final List<Integer> seed) throws Exception {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < seed.size(); i++) {
      if (i > 0) sb.append(',');
      sb.append(seed.get(i));
    }
    sb.append(']');
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
  }

  private static List<Integer> readArray(final JsonResourceSession session, final int revision) {
    final List<Integer> out = new ArrayList<>();
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      if (!rtx.hasFirstChild()) {
        return out;
      }
      rtx.moveToFirstChild();
      while (true) {
        out.add(rtx.getNumberValue().intValue());
        if (!rtx.hasRightSibling()) {
          break;
        }
        rtx.moveToRightSibling();
      }
    }
    return out;
  }

  // ==================== property parsing ====================

  private static long readSeedProperty() {
    final String raw = System.getProperty("sirix.fuzz.seed");
    if (raw == null || raw.isBlank()) {
      return DEFAULT_SEED;
    }
    final String trimmed = raw.trim();
    try {
      if (trimmed.startsWith("0x") || trimmed.startsWith("0X")) {
        return Long.parseUnsignedLong(trimmed.substring(2), 16);
      }
      return Long.parseLong(trimmed);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid sirix.fuzz.seed: '" + raw
          + "' (expected a decimal or 0x-prefixed hex long)", e);
    }
  }

  private static long readLongProperty(final String name, final long defaultValue) {
    final String raw = System.getProperty(name);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      final long value = Long.parseLong(raw.trim());
      if (value <= 0) {
        throw new IllegalArgumentException(name + " must be positive, was " + value);
      }
      return value;
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid " + name + ": '" + raw + "' (expected a positive long)", e);
    }
  }
}
