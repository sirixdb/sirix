package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedDiffBudgetTest {
  private static final int LIMIT = 4096;
  private static final String LARGE = "x".repeat(10_000);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  @TempDir
  Path directory;
  private ToolHandlers handlers;

  @BeforeEach
  void createDatabaseAndHandler() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("db")));
    assertThat(McpServerConfig.defaults(directory).maxStringValueLength()).isEqualTo(LIMIT);
    final var config = new McpServerConfig("test", "1", "stdio", directory.toString(), true, List.of(), List.of(),
        Map.of(), 1, LIMIT, false, false, false, null);
    final var access = new AccessControl(config);
    handlers = new ToolHandlers(config, access, new OutputSanitizer(config), new SnapshotRegistry(access),
        AuditLog.create(config));
  }

  @Test
  void fusedRenamesFitDespiteUnchangedLargeValuesAndAdvanceThePage() {
    for (final boolean paths : new boolean[] {true, false}) {
      final String resource = "renames-" + paths;
      create(resource, "{\"first\":\"" + LARGE + "\",\"second\":\"" + LARGE + "\"}", paths, writer -> {
        assertThat(writer.moveTo(2)).isTrue();
        assertThat(writer.getKind()).isEqualTo(NodeKind.OBJECT_NAMED_STRING);
        writer.setObjectKeyName("a");
        assertThat(writer.moveTo(3)).isTrue();
        assertThat(writer.getKind()).isEqualTo(NodeKind.OBJECT_NAMED_STRING);
        writer.setObjectKeyName("b");
      });
      for (int offset = 0; offset < 2; offset++) {
        final var page = page(resource, offset);
        final var update = payload(page, "update");
        assertThat(update.get("name")).isEqualTo(offset == 0
            ? "a"
            : "b");
        assertThat(update.containsKey("value")).isFalse();
        assertThat(update.containsKey("type")).isFalse();
        assertThat(update.containsKey("path")).isEqualTo(paths);
        assertContinuation(page, offset, 2);
      }
    }
  }

  @Test
  void allOperationKindsFitUnderLargeUnemittedAncestorNames() {
    create("operations", wrapped("[10,\"old\",20,0,30,0,40]"), false, writer -> {
      final long[] keys = arrayChildren(writer);
      writer.moveTo(keys[0]);
      writer.insertNumberValueAsLeftSibling(11);
      writer.moveTo(keys[1]);
      writer.remove();
      writer.moveTo(keys[2]);
      writer.insertNumberValueAsLeftSibling(22);
      writer.moveTo(keys[3]);
      writer.setNumberValue(33);
      writer.moveTo(keys[5]);
      writer.remove();
    });
    final List<String> kinds = List.of("insert", "replace", "update", "delete");
    for (int offset = 0; offset < kinds.size(); offset++) {
      final var page = page("operations", offset);
      final String kind = kinds.get(offset);
      final var operation = payload(page, kind);
      assertThat(operation.containsKey("path")).isFalse();
      if (!kind.equals("delete")) {
        assertThat(operation.get(kind.equals("update")
            ? "value"
            : "data")).isEqualTo(11 * (offset + 1));
      }
      assertContinuation(page, offset, kinds.size());
    }
    final var end = page("operations", kinds.size());
    assertThat((List<?>) end.get("diffs")).isEmpty();
    assertThat(end.get("truncated")).isEqualTo(false);
    assertThat(end.containsKey("continuation")).isFalse();
  }

  @Test
  void oversizedEmittedValuesAndFragmentsRemainWholeAndHaveActionableHints() {
    for (final String value : List.of(LARGE, "\n".repeat(2200))) {
      final String resource = value.equals(LARGE)
          ? "large-values"
          : "escaped-values";
      create(resource, wrapped("[10,\"old\",20,\"small\",30]"), false, writer -> {
        final long[] keys = arrayChildren(writer);
        writer.moveTo(keys[0]);
        writer.insertStringValueAsLeftSibling(value);
        writer.moveTo(keys[1]);
        writer.remove();
        writer.moveTo(keys[2]);
        writer.insertStringValueAsLeftSibling(value);
        writer.moveTo(keys[3]);
        writer.setStringValue(value);
      });
      for (int offset = 0; offset < 3; offset++)
        assertOversized(page(resource, offset), offset);
    }
    create("large-fragment", wrapped("[]"), false, writer -> {
      assertThat(writer.moveToFirstChild()).isTrue();
      assertThat(writer.moveToFirstChild()).isTrue();
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":\"" + LARGE + "\"}"),
          JsonNodeTrx.Commit.NO);
    });
    assertOversized(page("large-fragment", 0), 0);
    create("fitting-fragment", wrapped("[]"), false, writer -> {
      assertThat(writer.moveToFirstChild()).isTrue();
      assertThat(writer.moveToFirstChild()).isTrue();
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":\"small\"}"), JsonNodeTrx.Commit.NO);
    });
    final var fitting = page("fitting-fragment", 0);
    assertThat(payload(fitting, "insert").get("data")).isEqualTo("{\"value\":\"small\"}");
    assertContinuation(fitting, 0, 1);
  }

  @Test
  void multipleFragmentsShareScratchWithoutSharingContentOrRemainingBudgets() {
    create("fragments", "[]", false, writer -> {
      assertThat(writer.moveToFirstChild()).isTrue();
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"longer\":\"abcdef\"}"),
          JsonNodeTrx.Commit.NO);
      assertThat(writer.moveTo(1)).isTrue();
      assertThat(writer.moveToFirstChild()).isTrue();
      writer.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);
    });
    try (final var database = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = database.beginResourceSession("fragments")) {
      final var full = BoundedDiff.generate("db", session, 1, 2, 0, config(LIMIT));
      assertThat(full.get("truncated")).isEqualTo(false);
      final var changes = (List<?>) full.get("diffs");
      assertThat(changes).hasSize(2);
      final var document = MAPPER.valueToTree(changes);
      assertThat(document.get(0).get("insert").get("data").asText()).isEqualTo("{\"longer\":\"abcdef\"}");
      assertThat(document.get(1).get("insert").get("data").asText()).isEqualTo("{\"a\":1}");
      final int firstSize = MAPPER.writeValueAsString(List.of(changes.getFirst())).length();
      final var small = config(firstSize + 5);
      final var first = BoundedDiff.generate("db", session, 1, 2, 0, small);
      assertThat(first).containsEntry("diffs", List.of(changes.getFirst())).containsEntry("truncated", true);
      assertThat(((Map<?, ?>) first.get("continuation")).get("offset")).isEqualTo(1);
      final var second = BoundedDiff.generate("db", session, 1, 2, 1, small);
      assertThat(second).containsEntry("diffs", List.of(changes.get(1))).containsEntry("truncated", false);
    }
  }

  private McpServerConfig config(final int limit) {
    return new McpServerConfig("test", "1", "stdio", directory.toString(), true, List.of(), List.of(), Map.of(), 10,
        limit, false, false, false, null);
  }

  private void create(final String resource, final String initial, final boolean paths,
      final Consumer<JsonNodeTrx> changes) {
    try (final var database = Databases.openJsonDatabase(directory.resolve("db"))) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .buildPathSummary(paths)
                                                   .build());
      try (final var session = database.beginResourceSession(resource); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(initial), JsonNodeTrx.Commit.NO);
        writer.commit();
        writer.moveToDocumentRoot();
        changes.accept(writer);
        writer.commit();
      }
    }
  }

  private static long[] arrayChildren(final JsonNodeTrx writer) {
    assertThat(writer.moveToFirstChild()).isTrue();
    assertThat(writer.moveToFirstChild()).isTrue();
    assertThat(writer.isArray()).isTrue();
    final long[] keys = new long[(int) writer.getChildCount()];
    assertThat(writer.moveToFirstChild()).isTrue();
    for (int index = 0; index < keys.length; index++) {
      keys[index] = writer.getNodeKey();
      if (index + 1 < keys.length)
        assertThat(writer.moveToRightSibling()).isTrue();
    }
    return keys;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> page(final String resource, final int offset) {
    final CallToolResult response = handlers.diff(null, new CallToolRequest("sirix_diff",
        Map.of("database", "db", "resource", resource, "from_revision", 1, "to_revision", 2, "offset", offset)));
    assertThat(response.isError()).as(response.toString()).isFalse();
    final String text = ((TextContent) response.content().getFirst()).text();
    final var result = (Map<String, Object>) response.structuredContent();
    assertThat(MAPPER.readTree(text)).isEqualTo(MAPPER.valueToTree(result));
    assertThat(MAPPER.writeValueAsString(result.get("diffs")).length()).isLessThanOrEqualTo(LIMIT);
    return result;
  }

  private static Map<?, ?> payload(final Map<String, Object> page, final String kind) {
    final var operations = (List<?>) page.get("diffs");
    assertThat(operations).hasSize(1);
    final var wrapper = (Map<?, ?>) operations.getFirst();
    assertThat(wrapper).hasSize(1);
    assertThat(wrapper.containsKey(kind)).isTrue();
    return (Map<?, ?>) wrapper.get(kind);
  }

  private static void assertContinuation(final Map<String, Object> page, final int offset, final int total) {
    final boolean truncated = offset + 1 < total;
    assertThat(page.get("truncated")).isEqualTo(truncated);
    assertThat(page.containsKey("continuation")).isEqualTo(truncated);
    if (truncated)
      assertThat(((Map<?, ?>) page.get("continuation")).get("offset")).isEqualTo(offset + 1);
  }

  private static void assertOversized(final Map<String, Object> page, final int offset) {
    assertThat((List<?>) page.get("diffs")).isEmpty();
    assertThat(page.get("truncated")).isEqualTo(true);
    final var continuation = (Map<?, ?>) page.get("continuation");
    assertThat(continuation.get("offset")).isEqualTo(offset);
    assertThat(continuation.get("hint").toString()).contains("maxStringValueLength", "smaller projection");
  }

  private static String wrapped(final String array) {
    return "{\"" + LARGE + "\":" + array + "}";
  }
}
