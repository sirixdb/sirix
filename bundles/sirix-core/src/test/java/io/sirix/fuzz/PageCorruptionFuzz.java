package io.sirix.fuzz;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.io.IOStorage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.SplittableRandom;

import static io.sirix.JsonTestHelper.RESOURCE;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Page-corruption fuzz test in the spirit of SQLite's {@code dbfuzz2} — write a
 * resource, flip a random bit (or random byte) at a random offset in the
 * persisted data file, then attempt to read the resource. The contract under
 * test is <em>graceful failure</em>: Sirix must surface a checked exception
 * (preferably {@link io.sirix.exception.SirixException} / its subclasses or
 * {@link RuntimeException} originating in our code), not a JVM-level crash
 * (no SIGSEGV, no infinite loop, no silent wrong-answer corruption).
 *
 * <p>Note: this test is intentionally tolerant of "no exception thrown" on a
 * single iteration — many random byte flips land in unallocated regions or in
 * fields that don't break the next read. The failure conditions are:
 * <ol>
 *   <li>An iteration completes a read with success but yields a
 *       <em>different</em> document — silent corruption. (Detected by reading
 *       a sentinel field; if Sirix returns "the document" but its content has
 *       drifted, that's a silent-corruption bug.)</li>
 *   <li>Any iteration throws an unchecked exception that comes from the JVM
 *       runtime (e.g., {@link NullPointerException} without a Sirix-traceable
 *       message, {@link ArrayIndexOutOfBoundsException}, {@link Error}
 *       subclasses) — those indicate Sirix didn't validate input properly
 *       before dereferencing, which is exploitable for DoS.</li>
 * </ol>
 *
 * <p>Per-iteration determinism: a fixed master seed plus the iteration index
 * picks the corruption offset and the bit/byte to flip, so a failure prints
 * exactly the inputs needed to reproduce.
 */
final class PageCorruptionFuzz {

  private static final long MASTER_SEED = 0xC077B17EL;
  private static final int ITERATIONS = 32;
  /** Initial document the resource gets shredded with — a small but non-trivial
   *  JSON tree. Sentinel string {@code "fuzz-canary"} is checked after each
   *  successful (non-throwing) read; if it returns silently mutated, that's
   *  silent corruption. */
  private static final String SEED_DOCUMENT = """
      {"canary":"fuzz-canary","items":[1,2,3,4,5],"meta":{"version":1,"author":"sirix"}}
      """.strip();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Random bit-flips on the persisted data file must surface as exceptions, not JVM crashes")
  void pageCorruptionGracefulFailure() throws Exception {
    final SplittableRandom master = new SplittableRandom(MASTER_SEED);
    final Path dataFile = seedResource();
    final long fileSize = dataFile.toFile().length();
    if (fileSize <= 64) {
      fail("seed produced an unrealistically small data file (" + fileSize + " bytes) — generator issue");
    }

    int succeededReads = 0;
    int caughtExpected = 0;
    int caughtUnexpected = 0;
    final java.util.List<String> unexpectedFailures = new java.util.ArrayList<>();

    for (int i = 0; i < ITERATIONS; i++) {
      final long iterSeed = master.nextLong();
      final SplittableRandom rng = new SplittableRandom(iterSeed);
      // Skip the first 64 bytes — the file header; flipping bits there usually fails
      // very early without exercising any deserialization paths. The interesting
      // surface starts past it.
      final long offset = rng.nextLong(64L, fileSize);
      final int bit = rng.nextInt(0, 8);

      // Each iteration starts from a clean copy of the seeded resource, mutates a
      // single bit, then attempts to read. Because flips are localised, sometimes
      // the read happens to succeed (e.g., the byte was in a free area). Either
      // outcome is acceptable as long as Sirix doesn't crash.
      flipBit(dataFile, offset, bit);

      try {
        final String observed = readCanary();
        succeededReads++;
        // Silent-corruption check: if the read succeeded but the canary string is
        // not the one we wrote, Sirix returned a corrupted document without
        // signalling. That's the most dangerous failure mode.
        final int iterIdx = i;
        if (!"fuzz-canary".equals(observed)) {
          fail("iteration " + iterIdx + " seed=0x" + Long.toHexString(iterSeed)
                + " offset=" + offset + " bit=" + bit
                + ": canary read returned an unexpected mutated value: " + observed);
        }
      } catch (final SirixException | IllegalStateException | java.io.IOException
                     | java.util.concurrent.CompletionException expected) {
        // Sirix-traceable exception (or async-wrapped variant from CompletableFuture-driven
        // page-read paths). Acceptable.
        caughtExpected++;
      } catch (final NullPointerException | IndexOutOfBoundsException unexpected) {
        // Unchecked JVM-level exception caused by a missing input-validation step
        // — this is the exploitable failure mode the test is here to catch.
        caughtUnexpected++;
        unexpectedFailures.add(
            String.format("iteration %d seed=0x%s offset=%d bit=%d -> %s: %s",
                          i, Long.toHexString(iterSeed), offset, bit,
                          unexpected.getClass().getName(), unexpected.getMessage()));
      } finally {
        // Restore the file for the next iteration.
        flipBit(dataFile, offset, bit);
        JsonTestHelper.closeEverything();
      }
    }

    System.out.printf("[corruption-fuzz] iterations=%d succeeded_reads=%d caught_expected=%d caught_unexpected=%d%n",
                      ITERATIONS, succeededReads, caughtExpected, caughtUnexpected);

    if (!unexpectedFailures.isEmpty()) {
      fail("Sirix surfaced unchecked JVM exceptions on corrupted input — that's a graceful-failure violation.\n"
          + String.join("\n", unexpectedFailures));
    }
  }

  /**
   * Build a small JSON resource and return the path of its data file. The
   * resource has a single committed revision containing {@link #SEED_DOCUMENT}.
   */
  private static Path seedResource() throws Exception {
    // Don't try-with-resources the Database — JsonTestHelper.getDatabase returns
    // a cached singleton; closing it here makes subsequent getDatabase calls
    // return a closed instance. closeEverything() below handles teardown.
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(SEED_DOCUMENT), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
    JsonTestHelper.closeEverything();
    return PATHS.PATH1.getFile()
        .resolve("resources")
        .resolve(RESOURCE)
        .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve(IOStorage.FILENAME);
  }

  /** Flip one bit at the given offset. Self-inverse — calling twice restores. */
  private static void flipBit(final Path file, final long offset, final int bit) throws Exception {
    try (final RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      raf.seek(offset);
      final int original = raf.read();
      if (original < 0) {
        return; // past EOF — nothing to flip; iteration just no-ops.
      }
      raf.seek(offset);
      raf.write(original ^ (1 << bit));
    }
  }

  /**
   * Try to read the canary string. Throws on Sirix-detected corruption; returns
   * the observed canary value on success (which may have been silently mutated
   * if Sirix's checksum / structure validation missed the flip).
   */
  private static String readCanary() throws Exception {
    // Singleton-cached Database — kept outside try-with-resources to avoid
    // closing the cache; the per-iteration finally clause calls closeEverything().
    final var db = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      // Walk: doc-root → object → first object-record. The seed doc puts the
      // "canary" key first, so the canary value is reachable in 3 moves. If any
      // of these moves return a wrong node (e.g. corruption pointed to a
      // different node), getStringValue returns "" or some unrelated string —
      // that's the silent-corruption signal the caller asserts against.
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();    // outer object
      rtx.moveToFirstChild();    // ObjectKeyNode for "canary"
      rtx.moveToFirstChild();    // StringValue
      return rtx.getValue();
    }
  }
}
