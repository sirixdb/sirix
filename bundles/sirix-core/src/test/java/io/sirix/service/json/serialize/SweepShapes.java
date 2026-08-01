package io.sirix.service.json.serialize;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * The shared adversarial JSON corpus ({@code correctnessSweepShapes.json}) and the Jackson
 * configuration for exact-value comparisons over it — single-sourced so every test that sweeps the
 * corpus loads the same shapes (names included, for diagnostics) and parses with the same numeric
 * exactness. Used by {@link JsonCorrectnessSweepTest} and
 * {@link JsonSerializerRawBytesFastPathTest}.
 */
final class SweepShapes {

  private static final Path SHAPES_FILE =
      Paths.get("src", "test", "resources", "json", "correctnessSweepShapes.json");

  /** A test shape: a human-readable name plus the raw JSON document to exercise. */
  record Shape(String name, String json) {
  }

  /**
   * Mapper for exact round-trip comparison: big-integer/big-decimal modes keep exact values for
   * the numeric-edge shapes (2^53, 2^64, 2^128, 30+-digit integers, 40-digit decimals), so
   * {@code JsonNode.equals} enforces int-vs-float typing and value-exact numeric equality;
   * ALLOW_UNQUOTED_CONTROL_CHARS lets the raw-control-char shapes be read at all.
   */
  static ObjectMapper exactMapper() {
    return JsonMapper.builder()
                     .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                     .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                     .enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
                     .build();
  }

  /** Every shape of the corpus, in file order. */
  static List<Shape> loadShapes() throws Exception {
    final ObjectMapper plain = new ObjectMapper().enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
    final String text = Files.readString(SHAPES_FILE, StandardCharsets.UTF_8);
    final JsonNode arr = plain.readTree(text);
    final List<Shape> shapes = new ArrayList<>();
    for (final JsonNode node : arr) {
      shapes.add(new Shape(node.get("name").asText(), node.get("json").asText()));
    }
    return shapes;
  }

  private SweepShapes() {
  }
}
