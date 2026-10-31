package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledOnOs({OS.LINUX, OS.MAC})
class SnapshotAliasAuthorizationTest {
  private static final String LABEL = "target-only-label";
  private static final String SNAPSHOT_FILE = ".sirix-mcp-snapshots.json";
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  @TempDir
  Path directory;
  private Path root;
  private Path metadata;

  @BeforeEach
  void createDatabaseAndAliases() throws Exception {
    root = Files.createDirectory(directory.resolve("data"));
    final Path path = root.resolve("private");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var db = Databases.openJsonDatabase(path)) {
      for (final String resource : List.of("orders", "visible")) {
        db.createResource(ResourceConfiguration.newBuilder(resource).storageType(StorageType.FILE_CHANNEL).build());
        try (final var session = db.beginResourceSession(resource); final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":\"classified-value\"}"),
              JsonNodeTrx.Commit.NO);
          writer.commit("classified-commit-message");
        }
      }
    }
    new SnapshotRegistry(new AccessControl(config(root, false, Map.of()))).create("private", "orders", LABEL, 1);
    metadata = path.resolve(SNAPSHOT_FILE);
    Files.createSymbolicLink(root.resolve("public"), Path.of("private"));
    Files.createSymbolicLink(root.resolve("dangling"), Path.of("missing"));
    Files.createSymbolicLink(root.resolve("loop"), Path.of("loop"));
    Files.createSymbolicLink(root.resolve("escape"), directory);
    final Path resources = path.resolve("resources");
    Files.createSymbolicLink(resources.resolve("orders-alias"), Path.of("orders"));
    Files.createSymbolicLink(resources.resolve("dangling"), Path.of("missing"));
    Files.createSymbolicLink(resources.resolve("loop"), Path.of("loop"));
    Files.createSymbolicLink(resources.resolve("escape"), directory);
  }

  @Test
  void everyToolRejectsAliasedTargetsInBothModesWithoutDisclosureOrChanges() throws Exception {
    final var before = contents(root);
    for (final boolean readOnly : new boolean[] {false, true}) {
      final var handlers = handlers(config(root, readOnly, Map.of()));
      final var tools = tools(handlers);
      assertThat(tools).hasSize(14);
      for (final var tool : tools.entrySet()) {
        for (final String alias : List.of("public", "dangling", "loop", "escape"))
          assertAliasError(tool.getValue().apply(request(alias, "orders", Map.of())), tool.getKey());
        for (final String alias : List.of("orders-alias", "dangling", "loop", "escape"))
          assertAliasError(tool.getValue().apply(request("private", alias, Map.of())), tool.getKey());
      }
      assertContents(before, root);
    }
    Files.writeString(metadata, "{broken");
    final var registry = new SnapshotRegistry(new AccessControl(config(root, false, Map.of())));
    assertThatThrownBy(() -> registry.list("public", "orders")).isInstanceOf(AccessControl.AliasedNameException.class);
    assertThatThrownBy(() -> registry.resolve("private", "orders-alias", LABEL)).isInstanceOf(
        AccessControl.AliasedNameException.class);
    assertThatThrownBy(() -> registry.create("public", "orders", "new-label", 1)).isInstanceOf(
        AccessControl.AliasedNameException.class);
    assertThatThrownBy(() -> registry.delete("private", "orders-alias", LABEL)).isInstanceOf(
        AccessControl.AliasedNameException.class);
    assertThat(Files.readString(metadata)).isEqualTo("{broken");
  }

  @Test
  void listingsOmitAliasesAndRespectRealNameResourceRestrictions() throws Exception {
    for (final Map<String, List<String>> allowed : List.<Map<String, List<String>>>of(Map.of(),
        Map.of("private", List.of("visible")))) {
      final var handlers = handlers(config(root, false, allowed));
      final var databases = handlers.listDatabases(null, new CallToolRequest("test", Map.of()));
      assertThat(databases.isError()).as(text(databases)).isFalse();
      assertThat(MAPPER.readTree(text(databases))).isEqualTo(MAPPER.valueToTree(List.of("private")));
      final var resources = handlers.listResources(null, new CallToolRequest("test", Map.of("database", "private")));
      assertThat(resources.isError()).as(text(resources)).isFalse();
      final List<String> expected = allowed.isEmpty()
          ? List.of("orders", "visible")
          : List.of("visible");
      assertThat(MAPPER.readValue(text(resources), String[].class)).containsExactlyInAnyOrderElementsOf(expected);
      if (!allowed.isEmpty()) {
        assertThat(handlers.history(null, request("private", "orders", Map.of())).isError()).isTrue();
        assertThat(handlers.resourceInfo(null, request("private", "orders", Map.of())).isError()).isTrue();
      }
    }
  }

  @Test
  void queriesRejectNamesBeforeStoreCanonicalizationForJsonAndXml() throws Exception {
    final var writable = handlers(config(root, false, Map.of()));
    final var created =
        writable.createResource(null, request("xml", "orders", Map.of("format", "xml", "data", "<classified/>")));
    assertThat(created.isError()).as(text(created)).isFalse();
    Files.createSymbolicLink(root.resolve("xml-alias"), Path.of("xml"));
    Files.createSymbolicLink(root.resolve("xml/resources/orders-alias"), Path.of("orders"));
    final var before = contents(root);
    for (final boolean readOnly : new boolean[] {false, true}) {
      final var handlers = handlers(config(root, readOnly, Map.of()));
      for (final String query : List.of("count(jn:doc('public','orders'))", "count(jn:doc('private','orders-alias'))",
          "count(jn:collection('public'))", "count(xn:doc('" + root.resolve("xml-alias") + "','orders'))",
          "count(xn:doc('" + root.resolve("xml") + "','orders-alias'))")) {
        final var result =
            handlers.query(null, new CallToolRequest("test", Map.of("database", "private", "query", query)));
        assertAliasError(result, query);
      }
    }
    final var after = contents(root);
    assertThat(after.keySet()).containsExactlyInAnyOrderElementsOf(before.keySet());
    before.forEach((path, bytes) -> {
      if (!path.getFileName().toString().equals(".lock"))
        assertThat(after.get(path)).as(path.toString()).isEqualTo(bytes);
    });
  }

  @Test
  void realNameLabelsSurviveColdReopenWithoutMigratingAliasEntries() throws Exception {
    final Path linkedRoot = directory.resolve("linked-root");
    Files.createSymbolicLink(linkedRoot, root);
    Files.writeString(metadata, MAPPER.writeValueAsString(
        Map.of("orders", Map.of("real-label", 1), "orders-alias", Map.of("legacy-alias-label", 1))));
    final var config = config(linkedRoot.resolve("."), false, Map.of());
    final var registry = new SnapshotRegistry(new AccessControl(config));
    assertThat(registry.list("private", "orders")).containsExactlyEntriesOf(Map.of("real-label", 1));
    assertThat(registry.resolve("private", "orders", "legacy-alias-label")).isNull();
    assertThatThrownBy(() -> registry.resolve("private", "orders-alias", "legacy-alias-label")).isInstanceOf(
        AccessControl.AliasedNameException.class);
    registry.create("private", "orders", "new-label", 1);
    assertThat(MAPPER.readTree(Files.readString(metadata)).has("orders-alias")).isFalse();
    Databases.clearGlobalCaches();
    final var cold = handlers(config);
    final var listed = cold.listSnapshots(null, request("private", "orders", Map.of()));
    assertThat(listed.isError()).as(text(listed)).isFalse();
    assertThat(MAPPER.readTree(text(listed))).isEqualTo(MAPPER.valueToTree(Map.of("real-label", 1, "new-label", 1)));
    final var historical =
        cold.query(null, request("private", "orders", Map.of("query", "$$.value", "snapshot", "new-label")));
    assertThat(historical.isError()).as(text(historical)).isFalse();
    assertThat(text(historical)).contains("classified-value");
    final var deleted = cold.deleteSnapshot(null, request("private", "orders", Map.of("name", "new-label")));
    assertThat(deleted.isError()).as(text(deleted)).isFalse();
    assertThat(new SnapshotRegistry(new AccessControl(config)).list("private", "orders")).containsExactlyEntriesOf(
        Map.of("real-label", 1));
  }

  private static Map<String, Function<CallToolRequest, CallToolResult>> tools(final ToolHandlers h) {
    final var tools = new LinkedHashMap<String, Function<CallToolRequest, CallToolResult>>();
    tools.put("listDatabases", request -> h.listDatabases(null, request));
    tools.put("listResources", request -> h.listResources(null, request));
    tools.put("resourceInfo", request -> h.resourceInfo(null, request));
    tools.put("query", request -> h.query(null, request));
    tools.put("history", request -> h.history(null, request));
    tools.put("diff", request -> h.diff(null, request));
    tools.put("listSnapshots", request -> h.listSnapshots(null, request));
    tools.put("createSnapshot", request -> h.createSnapshot(null, request));
    tools.put("createResource", request -> h.createResource(null, request));
    tools.put("insert", request -> h.insert(null, request));
    tools.put("update", request -> h.update(null, request));
    tools.put("delete", request -> h.delete(null, request));
    tools.put("revert", request -> h.revert(null, request));
    tools.put("deleteSnapshot", request -> h.deleteSnapshot(null, request));
    return tools;
  }

  private static void assertAliasError(final CallToolResult result, final String context) {
    assertThat(result.isError()).as(context + ": " + text(result)).isTrue();
    assertThat((Map<?, ?>) result.structuredContent()).as(context).isNotNull();
    assertThat(((Map<?, ?>) result.structuredContent()).get("code")).as(context).isEqualTo("aliased_name");
    assertThat(text(result)).doesNotContain(LABEL, "classified-value", "classified-commit-message", "latestRevision");
  }

  private static McpServerConfig config(final Path root, final boolean readOnly,
      final Map<String, List<String>> resources) {
    return new McpServerConfig("test", "1", "stdio", root.toString(), readOnly, List.of(), List.of(), resources, 100,
        4096, false, false, false, null);
  }

  private static ToolHandlers handlers(final McpServerConfig config) throws Exception {
    final var access = new AccessControl(config);
    return new ToolHandlers(config, access, new OutputSanitizer(config), new SnapshotRegistry(access),
        AuditLog.create(config));
  }

  private static CallToolRequest request(final String database, final String resource,
      final Map<String, Object> parameters) {
    final var args = new LinkedHashMap<String, Object>(parameters);
    args.put("database", database);
    args.put("resource", resource);
    args.putIfAbsent("query", "$$");
    args.putIfAbsent("data", "{}");
    args.putIfAbsent("nodeKey", 2);
    args.putIfAbsent("value", "changed");
    if (!args.containsKey("snapshot"))
      args.putIfAbsent("revision", 1);
    args.putIfAbsent("name", LABEL);
    args.putIfAbsent("overwrite", true);
    return new CallToolRequest("test", args);
  }

  private static String text(final CallToolResult result) {
    return ((TextContent) result.content().getFirst()).text();
  }

  private static Map<Path, byte[]> contents(final Path root) throws Exception {
    final var result = new LinkedHashMap<Path, byte[]>();
    try (final var paths = Files.walk(root)) {
      for (final Path path : paths.filter(Files::isRegularFile).toList())
        result.put(root.relativize(path), Files.readAllBytes(path));
    }
    return result;
  }

  private static void assertContents(final Map<Path, byte[]> expected, final Path root) throws Exception {
    final var actual = contents(root);
    assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
    expected.forEach((path, bytes) -> assertThat(actual.get(path)).as(path.toString()).isEqualTo(bytes));
  }
}
