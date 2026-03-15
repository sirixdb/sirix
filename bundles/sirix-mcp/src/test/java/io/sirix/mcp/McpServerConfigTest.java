package io.sirix.mcp;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpServerConfigTest {

  @Test
  void defaultsAreReadOnly() {
    var config = McpServerConfig.defaults(Path.of("/tmp/test"));
    assertThat(config.readOnly()).isTrue();
  }

  @Test
  void withReadOnlyProducesModifiedCopy() {
    var config = McpServerConfig.defaults(Path.of("/tmp/test"));
    var rw = config.withReadOnly(false);
    assertThat(rw.readOnly()).isFalse();
    assertThat(rw.name()).isEqualTo(config.name());
    assertThat(rw.databasePath()).isEqualTo(config.databasePath());
    assertThat(rw.maxResultSize()).isEqualTo(config.maxResultSize());
  }

  @Test
  void emptyAllowListAllowsAllDatabases() {
    var config = McpServerConfig.defaults(Path.of("/tmp/test"));
    assertThat(config.isDatabaseAllowed("any")).isTrue();
  }

  @Test
  void denyListBlocksSpecificDatabases() {
    var config = new McpServerConfig(
        "test", "1.0.0", "stdio", "/tmp/test",
        true, List.of(), List.of("blocked"), Map.of(),
        100, 4096, true, true, false, null);
    assertThat(config.isDatabaseAllowed("blocked")).isFalse();
    assertThat(config.isDatabaseAllowed("other")).isTrue();
  }

  @Test
  void allowListRestrictsToOnlyListed() {
    var config = new McpServerConfig(
        "test", "1.0.0", "stdio", "/tmp/test",
        true, List.of("allowed"), List.of(), Map.of(),
        100, 4096, true, true, false, null);
    assertThat(config.isDatabaseAllowed("allowed")).isTrue();
    assertThat(config.isDatabaseAllowed("other")).isFalse();
  }

  @Test
  void resourceAllowlistEnforced() {
    var config = new McpServerConfig(
        "test", "1.0.0", "stdio", "/tmp/test",
        true, List.of(), List.of(), Map.of("db1", List.of("res1")),
        100, 4096, true, true, false, null);
    assertThat(config.isResourceAllowed("db1", "res1")).isTrue();
    assertThat(config.isResourceAllowed("db1", "res2")).isFalse();
    // Database without resource restrictions allows all
    assertThat(config.isResourceAllowed("db2", "anything")).isTrue();
  }
}
