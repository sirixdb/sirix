package io.sirix.service.json.serialize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression coverage for serializer bugs found by adversarial startNodeKey / multi-revision
 * sweeps:
 *
 * <ul>
 *   <li>a fused named PRIMITIVE as the start node emitted a bare {@code "name":value} fragment
 *       (no enclosing braces) in every metadata mode — REST-reachable via {@code ?nodeId=K};</li>
 *   <li>a fused named OBJECT/ARRAY as the start node leaked its opening wrapper brace in the
 *       non-metadata path ({@code {"n":{…}} } missing the final {@code }});</li>
 *   <li>the per-serializer quoted-key cache served a PRIOR revision's key text when a freed
 *       nameKey slot was re-assigned to a hash-colliding name in a later revision;</li>
 *   <li>{@link JsonRecordSerializer} concatenated one full payload per revision with no
 *       separator for multi-revision requests (invalid JSON) while every inner per-record
 *       serializer re-serialized ALL revisions;</li>
 *   <li>the byte sink's {@code append(char)} bridge corrupted surrogate pairs split across two
 *       calls;</li>
 *   <li>{@link JsonRecordSerializer}'s byte (OutputStream) pipeline must emit byte-for-byte what
 *       the Writer pipeline emits (equality gate);</li>
 *   <li>the limited serializer relabeled fused kinds to the legacy {@code OBJECT_KEY} in the
 *       metadata {@code type} field, so a node changed type when clients toggled maxLevel;</li>
 *   <li>{@code append(null)} on the output sinks NPE'd instead of appending "null" per the
 *       {@link Appendable} contract.</li>
 * </ul>
 */
public final class JsonSerializerStartNodeRegressionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  private enum MetaMode {
    NONE, NODE_KEY, NODE_KEY_AND_CHILD_COUNT, FULL
  }

  private static void applyMode(final JsonSerializer.Builder builder, final MetaMode mode) {
    switch (mode) {
      case NODE_KEY -> builder.withNodeKeyMetaData(true);
      case NODE_KEY_AND_CHILD_COUNT -> builder.withNodeKeyAndChildCountMetaData(true);
      case FULL -> builder.withMetaData(true);
      case NONE -> {
      }
    }
  }

  private static String serializeStartNode(final JsonResourceSession manager, final long startNodeKey,
      final MetaMode mode, final boolean limited, final boolean byteSink) {
    if (byteSink) {
      final var out = new ByteArrayOutputStream();
      final var builder = JsonSerializer.newBuilder(manager, out).startNodeKey(startNodeKey);
      applyMode(builder, mode);
      if (limited) {
        builder.maxLevel(9);
      }
      builder.build().call();
      return out.toString(StandardCharsets.UTF_8);
    }
    final var out = new StringWriter();
    final var builder = JsonSerializer.newBuilder(manager, out).startNodeKey(startNodeKey);
    applyMode(builder, mode);
    if (limited) {
      builder.maxLevel(9);
    }
    builder.build().call();
    return out.toString();
  }

  /** Member name → nodeKey for the top-level members of the canned shapes document. */
  private static Map<String, Long> topLevelMembers(final JsonResourceSession manager) {
    final Map<String, Long> targets = new LinkedHashMap<>();
    try (final var rtx = manager.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild();
      while (true) {
        targets.put(rtx.getName().stringValue(), rtx.getNodeKey());
        if (!rtx.hasRightSibling()) {
          break;
        }
        rtx.moveToRightSibling();
      }
    }
    return targets;
  }

  private static JsonResourceSession createShapesResource() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
    try (final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"obj\":{\"a\":1},\"arr\":[1,2],\"str\":\"s\",\"num\":7,\"boo\":true,\"nul\":null,"
              + "\"emptyobj\":{},\"emptyarr\":[]}"));
    }
    return manager;
  }

  @Test
  public void startNodeOnFusedMemberAlwaysProducesValidJson() throws Exception {
    try (final var manager = createShapesResource()) {
      final Map<String, Long> targets = topLevelMembers(manager);
      for (final var target : targets.entrySet()) {
        for (final MetaMode mode : MetaMode.values()) {
          for (final boolean limited : new boolean[] { false, true }) {
            for (final boolean byteSink : new boolean[] { false, true }) {
              final String label = "nodeId=" + target.getKey() + " mode=" + mode
                  + (limited ? " limited" : " unlimited") + (byteSink ? " byte" : " char");
              final String output =
                  serializeStartNode(manager, target.getValue(), mode, limited, byteSink);
              try {
                MAPPER.readTree(output);
              } catch (final Exception e) {
                throw new AssertionError(label + " produced invalid JSON: " + output, e);
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void fusedPrimitiveStartNodeIsBracedInPlainMode() {
    try (final var manager = createShapesResource()) {
      final Map<String, Long> targets = topLevelMembers(manager);
      assertEquals("{\"str\":\"s\"}",
                   serializeStartNode(manager, targets.get("str"), MetaMode.NONE, false, false));
      assertEquals("{\"num\":7}",
                   serializeStartNode(manager, targets.get("num"), MetaMode.NONE, false, false));
      // Structural members must close the wrapper they open.
      assertEquals("{\"obj\":{\"a\":1}}",
                   serializeStartNode(manager, targets.get("obj"), MetaMode.NONE, false, false));
      assertEquals("{\"arr\":[1,2]}",
                   serializeStartNode(manager, targets.get("arr"), MetaMode.NONE, false, false));
      assertEquals("{\"emptyobj\":{}}",
                   serializeStartNode(manager, targets.get("emptyobj"), MetaMode.NONE, false, false));
      assertEquals("{\"emptyarr\":[]}",
                   serializeStartNode(manager, targets.get("emptyarr"), MetaMode.NONE, false, false));
    }
  }

  @Test
  public void quotedKeyCacheDoesNotLeakKeyTextAcrossRevisions() throws Exception {
    // "Aa" and "BB" share the same String.hashCode(), so once revision 2 removes the only "Aa"
    // field the freed name-dictionary slot is re-assigned to "BB" in revision 3 — a serializer
    // key cache that survives the revision loop then prints "Aa" for revision 3's field.
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"Aa\":1}]"));
      }
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.remove();
        wtx.commit();
      }
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"BB\":2}"));
      }

      final var out = new StringWriter();
      JsonSerializer.newBuilder(manager, out, 1, 3).build().call();
      final JsonNode root = MAPPER.readTree(out.toString());
      final JsonNode revisionThree = root.get("sirix").get(1).get("revision").get(0);
      assertTrue("revision 3 must serialize its own key \"BB\", not revision 1's \"Aa\": " + out,
                 revisionThree.has("BB"));
      assertFalse(revisionThree.has("Aa"));
    }
  }

  @Test
  public void recordSerializerWrapsMultipleRevisionsInEnvelope() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"));
      }
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"b\":2}"));
      }

      // Multi-revision: one envelope, one payload per revision — was `}{`-concatenated garbage
      // with every record additionally re-serializing every revision.
      final var multi = new StringWriter();
      JsonRecordSerializer.newBuilder(manager, 10, multi, 1, 2).build().call();
      final JsonNode root = MAPPER.readTree(multi.toString());
      assertTrue("expected {\"sirix\":[…]} envelope: " + multi, root.has("sirix"));
      assertEquals(2, root.get("sirix").size());
      assertEquals(1, root.get("sirix").get(0).get("revisionNumber").asInt());
      assertEquals(2, root.get("sirix").get(1).get("revisionNumber").asInt());
      assertTrue(root.get("sirix").get(0).get("revision").get("a") != null);

      // Single revision keeps the bare payload shape paginating clients consume.
      final var single = new StringWriter();
      JsonRecordSerializer.newBuilder(manager, 10, single, 2).build().call();
      final JsonNode singleRoot = MAPPER.readTree(single.toString());
      assertFalse("single-revision output must stay un-enveloped: " + single, singleRoot.has("sirix"));
    }
  }

  /**
   * Equality gate for the {@link JsonRecordSerializer} byte pipeline: the OutputStream-based
   * builder must emit byte-for-byte what the Writer-based builder emits (UTF-8 encoded), across
   * {initial load, pagination from a mid sibling} × {plain, withNodeKeyAndChildCountMetaData} ×
   * numberOfRecords {1, 3}, over a document with nested objects/arrays/unicode/escapes.
   */
  @Test
  public void recordSerializerByteAndCharPipelinesEmitIdenticalBytes() throws Exception {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"alpha\":{\"nested\":{\"deep\":[1,2,{\"x\":\"y\"}]},\"empty\":{}},"
                + "\"beta\":[true,null,3.14,\"quote \\\" backslash \\\\ tab\\tnewline\\n\"],"
                + "\"gamma\":\"unicode äöü € 😀 中文\",\"delta\":42,\"eps\":null}"));
      }
      // Pagination resumes AFTER this top-level member (its right siblings are the page).
      final long midSibling = topLevelMembers(manager).get("beta");

      for (final boolean paginate : new boolean[] { false, true }) {
        for (final boolean withNodeKeyAndChildCount : new boolean[] { false, true }) {
          for (final int numberOfRecords : new int[] { 1, 3 }) {
            final String label = (paginate ? "pagination" : "initial load")
                + (withNodeKeyAndChildCount ? " nodeKeyAndChildCount" : " plain")
                + " numberOfRecords=" + numberOfRecords;

            final var writer = new StringWriter();
            final var charBuilder = JsonRecordSerializer.newBuilder(manager, numberOfRecords, writer);
            if (paginate) {
              charBuilder.startNodeKey(midSibling);
            }
            charBuilder.withNodeKeyAndChildCountMetaData(withNodeKeyAndChildCount).build().call();

            final var bytes = new ByteArrayOutputStream();
            final var byteBuilder = JsonRecordSerializer.newBuilder(manager, numberOfRecords, bytes);
            if (paginate) {
              byteBuilder.startNodeKey(midSibling);
            }
            byteBuilder.withNodeKeyAndChildCountMetaData(withNodeKeyAndChildCount).build().call();

            final String charOutput = writer.toString();
            try {
              MAPPER.readTree(charOutput);
            } catch (final Exception e) {
              throw new AssertionError(label + " produced invalid JSON: " + charOutput, e);
            }
            assertArrayEquals(label + " — byte pipeline must match char pipeline byte-for-byte",
                              charOutput.getBytes(StandardCharsets.UTF_8),
                              bytes.toByteArray());
          }
        }
      }

      // REST always sets maxLevel: the inner per-record serializers then DELEGATE to
      // JsonLimitedSerializer, which writes through the shared sink's Appendable bridge —
      // gate that path too (full metadata to maximize coverage; hashes are off here).
      for (final boolean paginate : new boolean[] { false, true }) {
        final var writer = new StringWriter();
        final var charBuilder = JsonRecordSerializer.newBuilder(manager, 3, writer);
        final var bytes = new ByteArrayOutputStream();
        final var byteBuilder = JsonRecordSerializer.newBuilder(manager, 3, bytes);
        if (paginate) {
          charBuilder.startNodeKey(midSibling);
          byteBuilder.startNodeKey(midSibling);
        }
        charBuilder.maxLevel(2).withMetaData(true).build().call();
        byteBuilder.maxLevel(2).withMetaData(true).build().call();
        assertArrayEquals("maxLevel+full metadata (" + (paginate ? "pagination" : "initial load") + ")",
                          writer.toString().getBytes(StandardCharsets.UTF_8), bytes.toByteArray());
      }

      // Multi-revision {"sirix":[…]} envelope through the byte pipeline.
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"second\":\"revision ü 😀\"}"));
      }
      final var writer = new StringWriter();
      JsonRecordSerializer.newBuilder(manager, 10, writer, 1, 2).build().call();
      final var bytes = new ByteArrayOutputStream();
      JsonRecordSerializer.newBuilder(manager, 10, bytes, 1, 2).build().call();
      assertArrayEquals("multi-revision envelope",
                        writer.toString().getBytes(StandardCharsets.UTF_8), bytes.toByteArray());
    }
  }

  /**
   * Wire consistency for the metadata {@code type} field: the SAME node must serialize with the
   * same concrete fused kind name (e.g. {@code OBJECT_NAMED_STRING}) whether or not a
   * maxLevel/maxChildren/maxNodes limit routes the request through {@link JsonLimitedSerializer}
   * — historically the limited path collapsed all fused kinds to the legacy {@code OBJECT_KEY}
   * label, so clients saw a node change type when toggling {@code maxLevel}.
   */
  @Test
  public void limitedSerializerEmitsSameConcreteTypeNamesAsUnlimited() throws Exception {
    try (final var manager = createShapesResource()) {
      final var unlimitedOut = new StringWriter();
      JsonSerializer.newBuilder(manager, unlimitedOut).withMetaData(true).build().call();

      final var limitedOut = new StringWriter();
      JsonSerializer.newBuilder(manager, limitedOut).withMetaData(true).maxLevel(9).build().call();

      final Map<Long, String> unlimitedTypes = new HashMap<>();
      collectNodeTypes(MAPPER.readTree(unlimitedOut.toString()), unlimitedTypes);
      final Map<Long, String> limitedTypes = new HashMap<>();
      collectNodeTypes(MAPPER.readTree(limitedOut.toString()), limitedTypes);

      assertFalse("no node may be relabeled to the legacy OBJECT_KEY under limits: " + limitedOut,
                  limitedTypes.containsValue("OBJECT_KEY"));
      assertTrue("expected concrete fused kinds in the sample document",
                 unlimitedTypes.containsValue("OBJECT_NAMED_STRING")
                     && unlimitedTypes.containsValue("OBJECT_NAMED_OBJECT")
                     && unlimitedTypes.containsValue("OBJECT_NAMED_ARRAY"));
      assertEquals("nodeKey→type mapping must not depend on the maxLevel toggle",
                   unlimitedTypes, limitedTypes);
    }
  }

  /** Collects every {@code nodeKey → type} pair from metadata objects in the serialized tree. */
  private static void collectNodeTypes(final JsonNode node, final Map<Long, String> types) {
    if (node == null) {
      return;
    }
    if (node.isObject() && node.has("nodeKey") && node.has("type")) {
      types.put(node.get("nodeKey").asLong(), node.get("type").asText());
    }
    for (final JsonNode child : node) {
      collectNodeTypes(child, types);
    }
  }

  /** Appendable contract: {@code append(null)} appends the four characters "null", not NPE. */
  @Test
  public void sinkAppendNullAppendsLiteralNullPerAppendableContract() throws Exception {
    final var bytes = new ByteArrayOutputStream();
    final JsonOutputSink byteSink = new JsonOutputSink.Utf8OutputSink(bytes);
    byteSink.append(null);
    byteSink.append(null, 1, 3); // as if csq were "null" -> "ul"
    byteSink.flush();
    assertEquals("nullul", bytes.toString(StandardCharsets.UTF_8));

    final var chars = new StringWriter();
    final JsonOutputSink charSink = new JsonOutputSink.CharOutputSink(chars);
    charSink.append(null);
    charSink.append(null, 1, 3);
    charSink.flush();
    assertEquals("nullul", chars.toString());
  }

  @Test
  public void utf8SinkPairsSurrogatesSplitAcrossAppendCalls() throws Exception {
    final var bytes = new ByteArrayOutputStream();
    final var sink = new JsonOutputSink.Utf8OutputSink(bytes);
    sink.append('\uD83D');
    sink.append('\uDE00');
    sink.flush();
    assertArrayEquals("😀".getBytes(StandardCharsets.UTF_8), bytes.toByteArray());

    // A genuinely lone high surrogate degrades exactly like the char pipeline's UTF-8 encode.
    final var lone = new ByteArrayOutputStream();
    final var loneSink = new JsonOutputSink.Utf8OutputSink(lone);
    loneSink.append('\uD83D');
    loneSink.append('x');
    loneSink.flush();
    assertArrayEquals(new byte[] { '?', 'x' }, lone.toByteArray());
  }
}
