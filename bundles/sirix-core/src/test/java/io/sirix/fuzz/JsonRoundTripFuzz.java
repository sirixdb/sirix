package io.sirix.fuzz;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Random-document round-trip fuzz test in the spirit of SQLite's {@code fuzzcheck} —
 * generate randomized but well-formed JSON, push it through the
 * shred → commit → serialize pipeline, and assert the streamed token sequence
 * of the output equals that of the input.
 *
 * <p>SQLite-style fuzzing has three flavours: workload fuzz (random valid SQL),
 * corruption fuzz (bit-flip files), and differential fuzz (compare against a
 * reference). This test is the workload-fuzz analogue: random valid input,
 * checked against the Jackson streaming-parser oracle (parse both input and
 * output, compare the token sequences). Streaming tokens encode structure +
 * scalar values + names without depending on whitespace or key insertion
 * order — Sirix's round-trip preserves both, so an exact token match is the
 * right contract.
 *
 * <p>Each iteration uses a deterministic seed derived from a fixed master
 * seed plus the iteration index, so a failure prints the iteration index and
 * the input that triggered it; reproducing locally is just re-running with
 * the same {@link #MASTER_SEED} and the iteration's seed printed in the
 * assertion message.
 *
 * <h2>Why streaming tokens, not Jackson's tree model</h2>
 *
 * <p>Sirix only depends on {@code jackson-core} (streaming API), not on
 * {@code jackson-databind} (tree model). Tokens give us the same structural
 * equivalence as a tree compare without the extra dependency. The trade-off
 * is that field ordering is not normalised — but Sirix's serializer emits
 * keys in the same order it shredded them, so for our generator (which
 * doesn't try to permute) the comparison is exact.
 */
final class JsonRoundTripFuzz {

  private static final long MASTER_SEED = 0xF422FF11L;
  private static final int ITERATIONS = 256;
  private static final int MAX_DEPTH = 5;
  private static final int MAX_BRANCH = 6;
  /** Single-byte ASCII printable range minus quote/backslash for cheap-and-safe random strings. */
  private static final int ASCII_LOW = 0x21;
  private static final int ASCII_HIGH = 0x7E;

  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Random JSON survives shred + commit + serialize, token-equivalent under Jackson streaming parse")
  void roundTrip() throws Exception {
    final SplittableRandom master = new SplittableRandom(MASTER_SEED);
    for (int i = 0; i < ITERATIONS; i++) {
      final long iterSeed = master.nextLong();
      final SplittableRandom rng = new SplittableRandom(iterSeed);
      final String input = generateJson(rng, 0);

      final List<String> inputTokens = tokenize(input);
      final String output = roundTripThroughSirix(input);
      final List<String> outputTokens = tokenize(output);

      // Lazy message: only formats on failure (256 iterations * a multi-line message
      // would otherwise allocate ~MB of dead strings).
      final int iter = i;
      assertEquals(inputTokens, outputTokens,
                   () -> "iteration " + iter + " seed=0x" + Long.toHexString(iterSeed)
                       + "\n  input  = " + input + "\n  output = " + output);

      JsonTestHelper.deleteEverything();
    }
  }

  private static String roundTripThroughSirix(final String input) throws Exception {
    // Note: don't try-with-resources the Database — JsonTestHelper.getDatabase
    // returns a cached singleton, so closing it in one block makes subsequent
    // getDatabase calls return a closed instance. tearDown's deleteEverything
    // handles teardown.
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

  /**
   * Tokenize a JSON string via Jackson's streaming parser and emit a sequence of
   * stable token descriptors. The descriptor for each token is a string like
   * {@code "OBJECT_START"}, {@code "FIELD:name"}, {@code "STRING:value"},
   * {@code "NUMBER:42"}. Two JSON inputs are token-equivalent under this
   * tokenisation iff they describe the same structure with the same scalar
   * values in the same iteration order, which is what Sirix's shred + serialize
   * round-trip is contracted to preserve.
   */
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
          case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT ->
              tokens.add("NUM:" + parser.getValueAsString());
          case VALUE_TRUE -> tokens.add("BOOL:true");
          case VALUE_FALSE -> tokens.add("BOOL:false");
          case VALUE_NULL -> tokens.add("NULL");
          default -> tokens.add("OTHER:" + t.name());
        }
      }
    }
    return tokens;
  }

  /**
   * Recursively generate a random JSON document. Branching is bounded by
   * {@link #MAX_DEPTH} and {@link #MAX_BRANCH}; leaves are typed null /
   * boolean / integer / string with equal probability. Depth 0 always returns
   * an object or array — Sirix's {@code insertSubtreeAsFirstChild} rejects
   * top-level scalars.
   */
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
      } while (!seen.add(key)); // dedupe — JSON objects forbid duplicate keys
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

  /**
   * ASCII printable, no quote/backslash. Sidesteps escape handling so a failure
   * is unambiguously a Sirix-side bug rather than a string-escape parity issue
   * between Jackson and Sirix's serializer.
   */
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
}
