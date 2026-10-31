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
    createDatabase(root.resolve("private"));
    new SnapshotRegistry(new AccessControl(config(root, List.of(), Map.of()))).create("private", "orders", LABEL, 1);
    metadata = root.resolve("private").resolve(SNAPSHOT_FILE);
    Files.createSymbolicLink(root.resolve("public"), Path.of("private"));
    Files.createSymbolicLink(root.resolve("private/resources/orders-alias"), Path.of("orders"));
  }

  @Test
  void deniedDatabaseAndResourceAliasesCannotDiscloseOrChangeLabels() throws Exception {
    final byte[] original = Files.readAllBytes(metadata);
    final List<McpServerConfig> policies = List.of(config(root, List.of("private"), Map.of()),
        config(root, List.of("public"), Map.of()), config(root, List.of(), Map.of("private", List.of("other"))),
        config(root, List.of(), Map.of("public", List.of("other"))),
        config(root, List.of(), Map.of("private", List.of("orders-alias"), "public", List.of("orders-alias"))));
    for (int index = 0; index < policies.size(); index++) {
      final String resource = index == policies.size() - 1
          ? "orders-alias"
          : "orders";
      assertDenied(policies.get(index), "public", resource);
      assertThat(Files.readAllBytes(metadata)).isEqualTo(original);
      assertThat(metadata.resolveSibling(SNAPSHOT_FILE + ".tmp")).doesNotExist();
    }
    final String corrupt = "{broken";
    Files.writeString(metadata, corrupt);
    assertDenied(policies.getFirst(), "public", "orders");
    assertThat(Files.readString(metadata)).isEqualTo(corrupt);
  }

  @Test
  void canonicalAliasesShareCacheResolutionAndColdPersistenceUnderASymlinkedRoot() throws Exception {
    final Path linkedRoot = directory.resolve("linked-root");
    Files.createSymbolicLink(linkedRoot, root);
    final var config = config(linkedRoot.resolve("."), List.of(),
        Map.of("public", List.of("orders", "orders-alias"), "private", List.of("orders", "orders-alias")));
    final var access = new AccessControl(config);
    final var registry = new SnapshotRegistry(access);
    final var handlers = handlers(config, access, registry);
    assertThat(registry.list("public", "orders")).containsExactlyEntriesOf(Map.of(LABEL, 1));
    final var created = handlers.createSnapshot(null, request("public", "orders", Map.of("name", "via-alias")));
    assertThat(created.isError()).as(text(created)).isFalse();
    assertThat(registry.resolve("private", "orders", "via-alias")).isEqualTo(1);
    registry.create("private", "orders", "via-canonical", 1);
    registry.create("public", "orders-alias", "via-resource-alias", 1);
    assertThat(registry.list("public", "orders")).containsEntry("via-canonical", 1)
                                                 .containsEntry("via-resource-alias", 1);
    assertThat(registry.resolve("private", "orders-alias", "via-alias")).isEqualTo(1);
    final var mutation = handlers.update(null, request("public", "orders", Map.of("nodeKey", 2, "value", "after")));
    assertThat(mutation.isError()).as(text(mutation)).isFalse();
    final var historical =
        handlers.query(null, request("public", "orders", Map.of("snapshot", "via-alias", "query", "$$.value")));
    assertThat(historical.isError()).as(text(historical)).isFalse();
    assertThat(text(historical)).contains("before").doesNotContain("after");
    final var diff =
        handlers.diff(null, request("public", "orders", Map.of("from_snapshot", "via-alias", "to_revision", 2)));
    assertThat(diff.isError()).as(text(diff)).isFalse();
    assertThat(MAPPER.readTree(text(diff)).get("diffs").isEmpty()).isFalse();
    final var revert =
        handlers.revert(null, request("public", "orders", Map.of("snapshot", "via-alias", "expected_head", 2)));
    assertThat(revert.isError()).as(text(revert)).isFalse();
    final var deleted = handlers.deleteSnapshot(null, request("public", "orders", Map.of("name", "via-canonical")));
    assertThat(deleted.isError()).as(text(deleted)).isFalse();
    assertThat(registry.resolve("private", "orders", "via-canonical")).isNull();
    assertThat(registry.delete("private", "orders-alias", LABEL)).isTrue();
    final var expected = Map.of("via-alias", 1, "via-resource-alias", 1);
    assertThat(registry.list("public", "orders")).containsExactlyInAnyOrderEntriesOf(expected);
    Databases.clearGlobalCaches();
    final var coldRegistry = new SnapshotRegistry(new AccessControl(config));
    final var cold = handlers(config, new AccessControl(config), coldRegistry);
    for (final String database : List.of("public", "private")) {
      assertThat(coldRegistry.list(database, "orders")).containsExactlyInAnyOrderEntriesOf(expected);
      assertThat(coldRegistry.resolve(database, "orders-alias", "via-resource-alias")).isEqualTo(1);
      final var listed = cold.listSnapshots(null, request(database, "orders", Map.of()));
      assertThat(listed.isError()).as(text(listed)).isFalse();
      assertThat(MAPPER.readTree(text(listed))).isEqualTo(MAPPER.valueToTree(expected));
    }
    assertThat(MAPPER.readTree(Files.readString(metadata)).has("orders-alias")).isFalse();
    assertThat(MAPPER.readTree(Files.readString(metadata)).get("orders")).isEqualTo(MAPPER.valueToTree(expected));
  }

  @Test
  void escapingDatabaseAndResourceAliasesAreDeniedEvenAfterAnAllowedAliasWasCached() throws Exception {
    final Path outside = directory.resolve("outside");
    createDatabase(outside);
    new SnapshotRegistry(new AccessControl(config(directory, List.of(), Map.of()))).create("outside", "orders", LABEL,
        1);
    final Path outsideMetadata = outside.resolve(SNAPSHOT_FILE);
    final byte[] outsideBefore = Files.readAllBytes(outsideMetadata);
    final byte[] insideBefore = Files.readAllBytes(metadata);
    Files.createSymbolicLink(root.resolve("escape"), outside);
    Files.createSymbolicLink(root.resolve("private/resources/escape"), outside.resolve("resources/orders"));
    final var config = config(root, List.of(), Map.of());
    assertDenied(config, "escape", "orders");
    assertDenied(config, "private", "escape");
    final var registry = new SnapshotRegistry(new AccessControl(config));
    assertThat(registry.resolve("public", "orders", LABEL)).isEqualTo(1);
    Files.delete(root.resolve("public"));
    Files.createSymbolicLink(root.resolve("public"), outside);
    assertThatThrownBy(() -> registry.list("public", "orders")).isInstanceOf(AccessControl.AccessDeniedException.class);
    Files.delete(root.resolve("public"));
    Files.createSymbolicLink(root.resolve("public"), Path.of("private"));
    assertThat(registry.resolve("public", "orders", LABEL)).isEqualTo(1);
    assertThat(Files.readAllBytes(metadata)).isEqualTo(insideBefore);
    assertThat(Files.readAllBytes(outsideMetadata)).isEqualTo(outsideBefore);
  }

  private static void assertDenied(final McpServerConfig config, final String database, final String resource)
      throws Exception {
    final var access = new AccessControl(config);
    final var registry = new SnapshotRegistry(access);
    assertThatThrownBy(() -> registry.list(database, resource)).isInstanceOf(AccessControl.AccessDeniedException.class);
    assertThatThrownBy(() -> registry.resolve(database, resource, LABEL)).isInstanceOf(
        AccessControl.AccessDeniedException.class);
    assertThatThrownBy(() -> registry.create(database, resource, "new-label", 1)).isInstanceOf(
        AccessControl.AccessDeniedException.class);
    assertThatThrownBy(() -> registry.delete(database, resource, LABEL)).isInstanceOf(
        AccessControl.AccessDeniedException.class);
    final var handlers = handlers(config, access, registry);
    for (final CallToolResult result : List.of(handlers.listSnapshots(null, request(database, resource, Map.of())),
        handlers.deleteSnapshot(null, request(database, resource, Map.of("name", LABEL))),
        handlers.createSnapshot(null, request(database, resource, Map.of("name", "new-label", "revision", 1))),
        handlers.query(null, request(database, resource, Map.of("snapshot", LABEL, "query", "$$"))),
        handlers.diff(null, request(database, resource, Map.of("from_snapshot", LABEL, "to_revision", 2))),
        handlers.revert(null, request(database, resource, Map.of("snapshot", LABEL))))) {
      assertThat(result.isError()).as(text(result)).isTrue();
      assertThat(text(result)).doesNotContain(LABEL).doesNotContain("Operation failed");
    }
  }

  private static McpServerConfig config(final Path root, final List<String> denied,
      final Map<String, List<String>> resources) {
    return new McpServerConfig("test", "1", "stdio", root.toString(), false, List.of(), denied, resources, 100, 4096,
        false, false, false, null);
  }

  private static ToolHandlers handlers(final McpServerConfig config, final AccessControl access,
      final SnapshotRegistry registry) throws Exception {
    return new ToolHandlers(config, access, new OutputSanitizer(config), registry, AuditLog.create(config));
  }

  private static CallToolRequest request(final String database, final String resource,
      final Map<String, Object> parameters) {
    final var args = new LinkedHashMap<String, Object>(parameters);
    args.put("database", database);
    args.put("resource", resource);
    return new CallToolRequest("test", args);
  }

  private static String text(final CallToolResult result) {
    return ((TextContent) result.content().getFirst()).text();
  }

  private static void createDatabase(final Path path) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var db = Databases.openJsonDatabase(path)) {
      db.createResource(ResourceConfiguration.newBuilder("orders").storageType(StorageType.FILE_CHANNEL).build());
      try (final var session = db.beginResourceSession("orders"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":\"before\"}"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
  }
}
