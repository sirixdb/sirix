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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import static java.util.Objects.requireNonNull;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedDiffNumericTest {
  private static final JsonMapper MAPPER =
      JsonMapper.builder().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();
  private static final List<Number> VALUES = List.of(new BigDecimal("0.12345678901234567890123456789"),
      new BigInteger("123456789012345678901234567890123456789"),
      new BigDecimal("-987654321.98765432109876543210987654321"),
      new BigInteger("-987654321098765432109876543210987654321"));

  @TempDir
  Path directory;

  @Test
  void numericOperationsRemainExactInBothRepresentationsAcrossPages() throws Exception {
    final Path path = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var db = Databases.openJsonDatabase(path)) {
      for (final String operation : List.of("insert", "replace", "update")) {
        db.createResource(ResourceConfiguration.newBuilder(operation).storageType(StorageType.FILE_CHANNEL).build());
        try (final var session = db.beginResourceSession(operation); final var writer = session.beginNodeTrx()) {
          final String initial = switch (operation) {
            case "insert" -> "[10,20,30,40]";
            case "replace" -> "[\"old\",10,\"old\",20,\"old\",30,\"old\",40]";
            default -> "[0,10,0,20,0,30,0,40]";
          };
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(initial), JsonNodeTrx.Commit.NO);
          writer.commit();
          writer.moveToDocumentRoot();
          assertThat(writer.moveToFirstChild()).isTrue();
          assertThat(writer.moveToFirstChild()).isTrue();
          final long[] keys = new long[operation.equals("insert")
              ? 4
              : 8];
          for (int index = 0; index < keys.length; index++) {
            keys[index] = writer.getNodeKey();
            if (index + 1 < keys.length)
              assertThat(writer.moveToRightSibling()).isTrue();
          }
          for (int index = 0; index < VALUES.size(); index++) {
            if (operation.equals("insert")) {
              assertThat(writer.moveTo(keys[index])).isTrue();
              writer.insertNumberValueAsLeftSibling(VALUES.get(index));
            } else {
              assertThat(writer.moveTo(keys[index * 2])).isTrue();
              if (operation.equals("update")) {
                writer.setNumberValue(VALUES.get(index));
              } else {
                writer.remove();
                assertThat(writer.moveTo(keys[index * 2 + 1])).isTrue();
                writer.insertNumberValueAsLeftSibling(VALUES.get(index));
              }
            }
          }
          writer.commit();
        }
      }
    }
    final var config = new McpServerConfig("test", "1", "stdio", directory.toString(), true, List.of(), List.of(),
        Map.of(), 2, 4096, false, false, false, null);
    final var handlers = new ToolHandlers(config, new AccessControl(config), new OutputSanitizer(config),
        new SnapshotRegistry(new AccessControl(config)), AuditLog.create(config));
    for (final String operation : List.of("insert", "replace", "update")) {
      for (final int offset : new int[] {0, 2}) {
        final var response = handlers.diff(null, new CallToolRequest("sirix_diff",
            Map.of("database", "db", "resource", operation, "from_revision", 1, "to_revision", 2, "offset", offset)));
        assertThat(response.isError()).as(response.toString()).isFalse();
        for (final Map<String, Object> page : List.of(structured(response), MAPPER.readValue(
            ((TextContent) response.content().getFirst()).text(), new TypeReference<Map<String, Object>>() {}))) {
          assertThat(page.get("truncated")).isEqualTo(offset == 0);
          if (offset == 0)
            assertThat(((Map<?, ?>) requireNonNull(page.get("continuation"))).get("offset")).isEqualTo(2);
          final var operations = (List<?>) requireNonNull(page.get("diffs"));
          assertThat(operations).hasSize(2);
          for (int index = 0; index < operations.size(); index++) {
            final var wrapper = (Map<?, ?>) requireNonNull(operations.get(index));
            assertThat(wrapper).hasSize(1);
            assertThat(wrapper.containsKey(operation)).isTrue();
            final var payload = (Map<?, ?>) requireNonNull(wrapper.get(operation));
            final Object value = payload.get(operation.equals("update")
                ? "value"
                : "data");
            assertThat(value).isNotNull().isInstanceOf(Number.class);
            assertThat(new BigDecimal(value.toString())).isEqualByComparingTo(VALUES.get(offset + index).toString());
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> structured(final CallToolResult result) {
    return (Map<String, Object>) result.structuredContent();
  }
}
