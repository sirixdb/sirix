package io.sirix.fuzz;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.temporal.AllTimeAxis;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential-style fuzz test: drive a Sirix resource with a sequence of
 * randomized mutations (insert/update/delete on random nodes within an array
 * counter), maintain a parallel in-memory expected model, and after every
 * commit verify both
 * <ol>
 *   <li>the latest revision exactly matches the expected model, and</li>
 *   <li>every prior revision still reads as it did when first committed
 *       — i.e., the bitemporal "frozen history" guarantee holds across
 *       all subsequent writes.</li>
 * </ol>
 *
 * <p>This is the third SQLite-style fuzz pattern (after workload-fuzz in
 * {@link JsonRoundTripFuzz} and corruption-fuzz in {@link PageCorruptionFuzz}):
 * <em>differential testing</em>, where the reference is an in-memory model
 * the test maintains alongside the engine. SQLite's TCL test suite does the
 * same with parallel SQL on a memory engine; for Sirix the natural reference
 * is a {@code List<Integer>} whose every state we record per commit.
 *
 * <p>Workload distribution intentionally mixes the three primitive write
 * operations Sirix has on number-array elements:
 * <ul>
 *   <li>{@code insertNumberValueAsRightSibling} — append-like.</li>
 *   <li>{@code setNumberValue} — point update.</li>
 *   <li>{@code remove} — point delete.</li>
 * </ul>
 *
 * Each iteration applies one operation and commits, producing a new revision.
 * The expected-model list snapshot at each commit is recorded so subsequent
 * reads at any revision can verify byte-equivalence.
 */
final class RandomWorkloadTemporalConsistencyFuzz {

  private static final long MASTER_SEED = 0xA055D1FFL;
  /** Number of write operations / commits. Each adds one revision. */
  private static final int OPERATIONS = 80;
  /** Initial array size. Big enough to give meaningful pick-a-random-index ops,
   *  small enough that walking the array per verification is cheap. */
  private static final int INITIAL_SIZE = 8;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Random insert/update/delete sequence: latest revision and all prior revisions stay consistent")
  void temporalConsistency() throws Exception {
    final SplittableRandom rng = new SplittableRandom(MASTER_SEED);
    // Reference model — index i in this list is the expected array contents at
    // revision (i + 2). Revision 1 is the seed (INITIAL_SIZE elements 0..N-1);
    // revision 2..N follows after each random op.
    final List<List<Integer>> expectedPerRevision = new ArrayList<>();

    // Seed: a number array [0, 1, ..., INITIAL_SIZE-1] in revision 1.
    final List<Integer> seed = new ArrayList<>(INITIAL_SIZE);
    for (int i = 0; i < INITIAL_SIZE; i++) {
      seed.add(i);
    }
    seedArray(seed);
    expectedPerRevision.add(new ArrayList<>(seed));

    // Live model the test mutates in lockstep with the resource.
    final List<Integer> live = new ArrayList<>(seed);
    int currentRevision = 1;

    for (int op = 0; op < OPERATIONS; op++) {
      // Pick a random operation. Distribution: 50% inserts, 30% updates, 20% deletes.
      // (Skewed toward insert so the array doesn't drain to empty quickly.)
      final int roll = rng.nextInt(0, 100);
      final OpKind kind;
      if (roll < 50) kind = OpKind.INSERT;
      else if (roll < 80) kind = OpKind.UPDATE;
      else kind = live.isEmpty() ? OpKind.INSERT : OpKind.DELETE;

      final int randomValue = rng.nextInt(-10_000, 10_001);
      final int randomIndex = live.isEmpty() ? 0 : rng.nextInt(0, live.size());

      switch (kind) {
        case INSERT -> applyInsert(randomIndex, randomValue, live);
        case UPDATE -> applyUpdate(randomIndex, randomValue, live);
        case DELETE -> applyDelete(randomIndex, live);
      }
      currentRevision++;
      expectedPerRevision.add(new ArrayList<>(live));
    }

    // Verify: read every revision back and compare element-for-element to the
    // recorded model. A divergence at any revision is either a Sirix bug or a
    // test-model bug; either way the test fails with the offending revision +
    // the expected vs observed lists.
    try (final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int rev = 1; rev <= currentRevision; rev++) {
        final List<Integer> expected = expectedPerRevision.get(rev - 1);
        final List<Integer> observed = readArray(session, rev);
        final int finalRev = rev;
        assertEquals(expected, observed,
                     () -> "revision " + finalRev + " diverges:\n  expected = " + expected + "\n  observed = " + observed);
      }

      // Bitemporal sanity: AllTimeAxis on the array root should yield exactly
      // currentRevision rtxs (one per revision in which the node existed).
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(currentRevision)) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // the array
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
    assertTrue(currentRevision >= OPERATIONS, "did not produce a revision per op");
  }

  private enum OpKind { INSERT, UPDATE, DELETE }

  private static void applyInsert(final int randomIndex, final int value, final List<Integer> live)
      throws Exception {
    try (final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      // Move to position randomIndex inside the array; insert as right sibling.
      // If the array is empty, fall back to insertAsFirstChild on the array node.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      if (live.isEmpty()) {
        wtx.insertNumberValueAsFirstChild(value);
        live.add(0, value);
      } else {
        wtx.moveToFirstChild(); // first element
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
    try (final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // first element
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
    try (final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // first element
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
    try (final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
  }

  /**
   * Read the integer array at the given revision and return its elements as a
   * {@code List<Integer>} for direct equals-comparison with the reference model.
   */
  private static List<Integer> readArray(final JsonResourceSession session, final int revision) {
    final List<Integer> out = new ArrayList<>();
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // array
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
}
