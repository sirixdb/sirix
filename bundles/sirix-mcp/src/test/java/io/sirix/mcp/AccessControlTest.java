package io.sirix.mcp;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AccessControlTest {

  private static McpServerConfig configWith(boolean readOnly,
      List<String> allow, List<String> deny, Map<String, List<String>> allowRes) {
    return new McpServerConfig(
        "test", "1.0.0", "stdio", "/tmp/test",
        readOnly, allow, deny, allowRes,
        100, 4096, true, true, false, null);
  }

  @Test
  void allowsAllDatabasesWhenNoListsConfigured() {
    var ac = new AccessControl(configWith(true, List.of(), List.of(), Map.of()));
    assertThatCode(() -> ac.checkDatabaseAccess("anydb")).doesNotThrowAnyException();
    assertThatCode(() -> ac.checkAccess("anydb", "anyres")).doesNotThrowAnyException();
  }

  @Test
  void deniesAccessWhenDatabaseInDenyList() {
    var ac = new AccessControl(configWith(true, List.of(), List.of("secret"), Map.of()));
    assertThatThrownBy(() -> ac.checkDatabaseAccess("secret"))
        .isInstanceOf(AccessControl.AccessDeniedException.class);
    assertThatThrownBy(() -> ac.checkAccess("secret", "res"))
        .isInstanceOf(AccessControl.AccessDeniedException.class);
  }

  @Test
  void allowsOnlyExplicitlyAllowedDatabases() {
    var ac = new AccessControl(configWith(true, List.of("allowed"), List.of(), Map.of()));
    assertThatCode(() -> ac.checkDatabaseAccess("allowed")).doesNotThrowAnyException();
    assertThatThrownBy(() -> ac.checkDatabaseAccess("other"))
        .isInstanceOf(AccessControl.AccessDeniedException.class);
  }

  @Test
  void denyListTakesPrecedenceOverAllowList() {
    var ac = new AccessControl(configWith(true, List.of("db1"), List.of("db1"), Map.of()));
    assertThatThrownBy(() -> ac.checkDatabaseAccess("db1"))
        .isInstanceOf(AccessControl.AccessDeniedException.class);
  }

  @Test
  void resourceAllowlistRestrictsAccess() {
    var ac = new AccessControl(configWith(true, List.of(), List.of(),
        Map.of("mydb", List.of("allowed_res"))));
    assertThatCode(() -> ac.checkAccess("mydb", "allowed_res")).doesNotThrowAnyException();
    assertThatThrownBy(() -> ac.checkAccess("mydb", "forbidden_res"))
        .isInstanceOf(AccessControl.AccessDeniedException.class);
  }

  @Test
  void resourceWildcardAllowsAll() {
    var ac = new AccessControl(configWith(true, List.of(), List.of(),
        Map.of("mydb", List.of("*"))));
    assertThatCode(() -> ac.checkAccess("mydb", "anything")).doesNotThrowAnyException();
  }

  @Test
  void readOnlyBlocksWriteAccess() {
    var ac = new AccessControl(configWith(true, List.of(), List.of(), Map.of()));
    assertThatThrownBy(() -> ac.checkWriteAccess())
        .isInstanceOf(AccessControl.AccessDeniedException.class);
  }

  @Test
  void readWriteAllowsWriteAccess() {
    var ac = new AccessControl(configWith(false, List.of(), List.of(), Map.of()));
    assertThatCode(() -> ac.checkWriteAccess()).doesNotThrowAnyException();
  }
}
