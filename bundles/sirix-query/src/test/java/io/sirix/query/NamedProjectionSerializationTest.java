package io.sirix.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for serializing a query result that IS a single fused named member through
 * {@link JsonDBSerializer} (the REST path: {@code isXQueryResultSequence().serializeTimestamp(true)}).
 *
 * <p>The bug: a projection like {@code jn:doc(...).products[0].id} yields an {@code OBJECT_NAMED_*}
 * node, which serializes inline as a bare {@code "name":value} fragment. Emitted directly after the
 * {@code "revision":} key of the result-metadata wrapper that produced INVALID JSON —
 * {@code {"revisionNumber":N,...,"revision":"id":"A"}} (two colons). The serializer now wraps such a
 * result in an object: {@code "revision":{"id":"A"}}.
 */
public final class NamedProjectionSerializationTest {

  private static final Path PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final String STORE =
      "jn:store('json-path1','mydoc.jn','"
          + "{\"products\":[{\"id\":\"A\",\"price\":10,\"specs\":{\"color\":\"red\"},\"tags\":[\"x\",\"y\"]},"
          + "{\"id\":\"B\",\"price\":20}]}')";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** Serialize a query exactly as the REST API does (JsonDBSerializer wraps in {@code {"rest":[…]}}). */
  private String serializeViaRest(final String query) throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, STORE).evaluate(ctx);
      final var sb = new StringBuilder();
      new Query(chain, query).serialize(ctx, new JsonDBSerializer(sb, false));
      return sb.toString();
    }
  }

  private static JsonObject firstResult(final String serialized) {
    final var rest = JsonParser.parseString(serialized).getAsJsonObject().getAsJsonArray("rest");
    assertEquals(1, rest.size(), "expected exactly one result item");
    return rest.get(0).getAsJsonObject();
  }

  @Test
  void namedStringProjectionSerializesAsValidJson() throws IOException {
    // Index is 0-based: products[0].id == "A".
    final var item = firstResult(serializeViaRest("jn:doc('json-path1','mydoc.jn').products[0].id"));
    assertTrue(item.get("revision").isJsonObject(),
        "a named-member result must be wrapped in an object, not emitted as a bare member");
    assertEquals("A", item.getAsJsonObject("revision").get("id").getAsString());
  }

  @Test
  void namedObjectProjectionSerializesAsValidJson() throws IOException {
    // Regression: alpha21 over-wrapped OBJECT_NAMED_OBJECT -> "revision":{{"specs":{…}}} (invalid).
    final var item = firstResult(serializeViaRest("jn:doc('json-path1','mydoc.jn').products[0].specs"));
    assertTrue(item.get("revision").isJsonObject(), "named object projection must be valid JSON");
    assertEquals("red", item.getAsJsonObject("revision").getAsJsonObject("specs").get("color").getAsString());
  }

  @Test
  void namedArrayProjectionSerializesAsValidJson() throws IOException {
    // Regression: alpha21 over-wrapped OBJECT_NAMED_ARRAY -> "revision":{{"tags":[…]}} (invalid).
    final var item = firstResult(serializeViaRest("jn:doc('json-path1','mydoc.jn').products[0].tags"));
    assertTrue(item.get("revision").isJsonObject());
    assertEquals(2, item.getAsJsonObject("revision").getAsJsonArray("tags").size());
  }

  @Test
  void computedStringWithSpecialCharsIsEscaped() throws IOException {
    // An atomic string result containing a quote/tab must be escaped (valid JSON), not raw.
    final var serialized = serializeViaRest(
        "concat('he said ', codepoints-to-string(34), 'hi', codepoints-to-string(34))");
    // Must parse as valid JSON (would throw before the fix).
    final var rest = JsonParser.parseString(serialized).getAsJsonObject().getAsJsonArray("rest");
    assertEquals("he said \"hi\"", rest.get(0).getAsString());
  }

  @Test
  void namedNumberProjectionSerializesAsValidJson() throws IOException {
    final var item = firstResult(serializeViaRest("jn:doc('json-path1','mydoc.jn').products[1].price"));
    assertTrue(item.get("revision").isJsonObject());
    assertEquals(20, item.getAsJsonObject("revision").get("price").getAsInt());
  }

  @Test
  void wholeObjectProjectionStillSerializesAsValidJson() throws IOException {
    // The non-named case (an unnamed object) must remain a plain object value, unchanged.
    final var item = firstResult(serializeViaRest("jn:doc('json-path1','mydoc.jn').products[0]"));
    final var revision = item.getAsJsonObject("revision");
    assertEquals("A", revision.get("id").getAsString());
    assertEquals(10, revision.get("price").getAsInt());
  }
}
