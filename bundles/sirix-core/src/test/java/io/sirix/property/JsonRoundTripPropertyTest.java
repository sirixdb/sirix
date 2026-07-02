package io.sirix.property;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;

import java.io.StringWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based (generative) correctness tests for the JSON shred → store → serialize pipeline.
 *
 * <p>Unlike the fixed-corpus {@code JsonCorrectnessSweepTest} and the seed-logged
 * {@code JsonStructuralFuzzTest}, these properties let jqwik generate <em>arbitrary</em> JSON
 * documents (bounded depth/size) and shrink any failure to a minimal counterexample. Two
 * invariants are checked:
 *
 * <ol>
 *   <li><b>Round-trip fidelity</b> — any generated document, shredded into a fresh resource and
 *       serialized back, is semantically identical to the input (objects key-order-insensitive,
 *       arrays order-sensitive, numbers by exact value via BigInteger/BigDecimal parsing).</li>
 *   <li><b>Revision immutability</b> — committing new revisions never changes the serialized
 *       output of any earlier revision (the core temporal-storage guarantee), and revision
 *       timestamps are monotonically non-decreasing.</li>
 * </ol>
 *
 * <p>On failure jqwik reports the seed and the shrunk counterexample; re-run with
 * {@code @Property(seed = "...")} to reproduce.
 */
class JsonRoundTripPropertyTest {

  private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;

  /**
   * Exact-value mapper: integers as BigInteger, floats as BigDecimal, so numeric fidelity is
   * checked without silent double rounding (same configuration as JsonCorrectnessSweepTest).
   */
  private static final ObjectMapper EXACT_MAPPER = JsonMapper.builder()
      .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
      .build();

  // ==================== PROPERTY 1: ROUND-TRIP FIDELITY ====================

  @Property(tries = 40)
  void shredThenSerializeRoundTripsAnyDocument(@ForAll("jsonContainers") final JsonNode document) throws Exception {
    final String input = document.toString();
    JsonTestHelper.deleteEverything();
    try {
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(input),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        final String output = serialize(session, session.getMostRecentRevisionNumber());
        final JsonNode expected = EXACT_MAPPER.readTree(input);
        final JsonNode actual = EXACT_MAPPER.readTree(output);
        assertEquals(expected, actual,
            () -> "Round-trip mismatch.\ninput:  " + input + "\noutput: " + output);
      }
    } finally {
      JsonTestHelper.deleteEverything();
    }
  }

  // ==================== PROPERTY 2: REVISION IMMUTABILITY ====================

  @Property(tries = 15)
  void committingNewRevisionsNeverAltersEarlierRevisions(
      @ForAll("jsonContainerLists") final List<JsonNode> documents,
      @ForAll final long seed) throws Exception {
    JsonTestHelper.deleteEverything();
    try {
      final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final Random random = new Random(seed);
        // Maps revision number -> serialized output captured right after that revision's commit.
        final List<int[]> revisionNumbers = new ArrayList<>(documents.size() + 1);
        final List<String> snapshots = new ArrayList<>(documents.size() + 1);

        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertArrayAsFirstChild();
          wtx.commit();
          revisionNumbers.add(new int[] {session.getMostRecentRevisionNumber()});
          snapshots.add(serialize(session, session.getMostRecentRevisionNumber()));

          for (final JsonNode document : documents) {
            wtx.moveToDocumentRoot();
            assertTrue(wtx.moveToFirstChild(), "document root must have the array child");
            // Occasionally delete the array's first element before appending, so revisions
            // exercise removals as well as inserts.
            if (random.nextInt(3) == 0 && wtx.hasFirstChild()) {
              wtx.moveToFirstChild();
              wtx.remove();
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
            }
            wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(document.toString()),
                JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.NO);
            wtx.commit();
            revisionNumbers.add(new int[] {session.getMostRecentRevisionNumber()});
            snapshots.add(serialize(session, session.getMostRecentRevisionNumber()));
          }
        }

        // Every earlier revision must serialize byte-identically to the snapshot taken
        // immediately after its commit — later commits must not have altered it.
        for (int i = 0; i < snapshots.size(); i++) {
          final int revision = revisionNumbers.get(i)[0];
          final String now = serialize(session, revision);
          assertEquals(snapshots.get(i), now,
              "Revision " + revision + " changed after later commits (temporal immutability violated)");
        }

        // Revision timestamps must be monotonically non-decreasing.
        Instant previous = null;
        for (final int[] revision : revisionNumbers) {
          try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision[0])) {
            final Instant timestamp = rtx.getRevisionTimestamp();
            if (previous != null) {
              assertFalse(timestamp.isBefore(previous),
                  "Revision timestamps must not decrease: " + previous + " -> " + timestamp);
            }
            previous = timestamp;
          }
        }
      }
    } finally {
      JsonTestHelper.deleteEverything();
    }
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws Exception {
    try (final Writer writer = new StringWriter()) {
      new JsonSerializer.Builder(session, writer, revision).build().call();
      return writer.toString();
    }
  }

  // ==================== GENERATORS ====================

  @Provide
  Arbitrary<JsonNode> jsonContainers() {
    return containerNode(3);
  }

  @Provide
  Arbitrary<List<JsonNode>> jsonContainerLists() {
    return containerNode(2).list().ofMinSize(1).ofMaxSize(4);
  }

  /** A container (object or array) whose descendants may nest up to {@code depth} levels. */
  private Arbitrary<JsonNode> containerNode(final int depth) {
    return Arbitraries.oneOf(objectNode(depth), arrayNode(depth));
  }

  private Arbitrary<JsonNode> jsonValue(final int depth) {
    if (depth <= 0) {
      return leafNode();
    }
    return Arbitraries.frequencyOf(
        Tuple.of(4, leafNode()),
        Tuple.of(1, Arbitraries.lazy(() -> objectNode(depth - 1))),
        Tuple.of(1, Arbitraries.lazy(() -> arrayNode(depth - 1))));
  }

  private Arbitrary<JsonNode> objectNode(final int depth) {
    return Arbitraries.maps(keyStrings(), jsonValue(depth - 1)).ofMaxSize(5).map(map -> {
      final ObjectNode object = FACTORY.objectNode();
      for (final Map.Entry<String, JsonNode> entry : map.entrySet()) {
        object.set(entry.getKey(), entry.getValue());
      }
      return object;
    });
  }

  private Arbitrary<JsonNode> arrayNode(final int depth) {
    return jsonValue(depth - 1).list().ofMaxSize(5).map(values -> {
      final ArrayNode array = FACTORY.arrayNode();
      for (final JsonNode value : values) {
        array.add(value);
      }
      return array;
    });
  }

  private Arbitrary<JsonNode> leafNode() {
    return Arbitraries.oneOf(
        valueStrings().map(FACTORY::textNode),
        Arbitraries.integers().map(FACTORY::numberNode),
        Arbitraries.longs().map(FACTORY::numberNode),
        Arbitraries.doubles().map(FACTORY::numberNode),
        bigIntegers().map(FACTORY::numberNode),
        Arbitraries.of(true, false).map(FACTORY::booleanNode),
        Arbitraries.just(FACTORY.nullNode()));
  }

  private Arbitrary<BigInteger> bigIntegers() {
    // Beyond long range: exercises the exact-integer storage path (2^63 .. ~2^127).
    return Arbitraries.longs().map(l -> BigInteger.valueOf(l).multiply(BigInteger.valueOf(Long.MAX_VALUE)));
  }

  private Arbitrary<String> valueStrings() {
    final Arbitrary<String> basic = Arbitraries.strings()
        .withCharRange((char) 0x20, (char) 0x7E)
        .withCharRange((char) 0x00, (char) 0x1F)
        .withCharRange(' ', '⿿')
        .ofMaxLength(12);
    // Fixed exotic samples: astral surrogate pairs, CJK, bidi marks (as escapes — raw
    // directionality characters in source trip Error Prone's UnicodeDirectionalityCharacters),
    // quotes/backslashes.
    final Arbitrary<String> exotic = Arbitraries.of("😀🦄", "𝒜𝒱", "你好世界",
        "\u202Ebidi\u202C", "\"quoted\\back\\\\slash\"", "");
    return Arbitraries.frequencyOf(
        Tuple.of(5, basic),
        Tuple.of(1, exotic));
  }

  private Arbitrary<String> keyStrings() {
    final Arbitrary<String> basic = Arbitraries.strings()
        .withCharRange((char) 0x20, (char) 0x7E)
        .ofMinLength(0)
        .ofMaxLength(8);
    final Arbitrary<String> exotic = Arbitraries.of("", "key with spaces", "ключ", "键", "a/b~c", "\"k\"");
    return Arbitraries.frequencyOf(
        Tuple.of(5, basic),
        Tuple.of(1, exotic));
  }
}
