package io.sirix.fuzz;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
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
 * resource, flip a random bit at a random offset in the persisted data file,
 * then attempt to read the resource. The contract under test is
 * <em>graceful failure</em>: Sirix must not crash the JVM, hang, or return
 * silently wrong data. Any thrown {@link Exception} is acceptable — surfacing
 * the corruption is the correct outcome regardless of which exception type
 * carries it (Sirix's own checksum-mismatch {@code SirixCorruptionException},
 * {@code IndexOutOfBoundsException} from a {@code MemorySegment} bounds-check
 * on a corrupted offset, {@code IllegalArgumentException} from an
 * oversize-allocation guard, async wrappers, etc.).
 *
 * <p>The test fails on:
 * <ol>
 *   <li><strong>Silent corruption</strong>: a read succeeds but the canary
 *       field has drifted — Sirix returned a corrupted document without
 *       signalling. Detected by comparing the canary value to a known
 *       sentinel.</li>
 *   <li><strong>Error-subclass throws</strong> (e.g. {@code OutOfMemoryError},
 *       {@code StackOverflowError}): we deliberately let these propagate so
 *       the test fails — they signal Sirix didn't bound resource use under
 *       crafted-input pressure.</li>
 *   <li><strong>JVM crash</strong>: auto-detected by the test runner as
 *       process death.</li>
 * </ol>
 *
 * <p>Many iterations land in unallocated regions or fields that don't break
 * the next read; that's expected — the seeded determinism (master seed plus
 * iteration index picks the offset and bit) means any failure prints the
 * inputs needed to reproduce.
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
    int caughtExceptions = 0;

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
      } catch (final Exception graceful) {
        // Any exception is a graceful failure — Sirix surfaced the corruption rather than
        // crashing the JVM, returning silently wrong data, or hanging. The specific type
        // doesn't matter for the contract: SirixException, SirixRuntimeException
        // (e.g. SirixCorruptionException from checksum mismatch), CompletionException
        // wrappers, IndexOutOfBoundsException from MemorySegment bounds-checks, and
        // IllegalArgumentException from oversize-allocation guards are all valid.
        // A real graceful-failure violation would be Error-subclass throws (e.g. OOME
        // from unbounded allocation, StackOverflowError from infinite recursion) — those
        // we deliberately let propagate so the test fails. A SIGSEGV is auto-detected
        // by the test runner (process death).
        caughtExceptions++;
      } finally {
        // Restore the file for the next iteration.
        flipBit(dataFile, offset, bit);
        JsonTestHelper.closeEverything();
      }
    }

    System.out.printf("[corruption-fuzz] iterations=%d succeeded_reads=%d caught_exceptions=%d%n",
                      ITERATIONS, succeededReads, caughtExceptions);
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
