package io.sirix.mcp;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.sirix.access.DatabaseConfiguration.DatabasePaths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SnapshotRegistryTest {

  @TempDir
  Path tempDir;

  private SnapshotRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new SnapshotRegistry(new AccessControl(McpServerConfig.defaults(tempDir)));
  }

  @Test
  void createAndResolveSnapshot() {
    registry.create("db1", "res1", "baseline", 5);
    assertThat(registry.resolve("db1", "res1", "baseline")).isEqualTo(5);
  }

  @Test
  void caseInsensitiveResourceAliasesSharePersistedSnapshotKeys() throws IOException {
    final Path database = tempDir.resolve("db");
    final Path resources = database.resolve(DatabasePaths.DATA.getFile());
    Files.createDirectories(resources.resolve("orders"));
    try (final FileSystem windows = Jimfs.newFileSystem(Configuration.windows())) {
      final Path windowsResources = windows.getPath("C:\\data\\db\\resources");
      Files.createDirectories(windowsResources.resolve("orders"));
      final AccessControl access = mock(AccessControl.class);
      when(access.databasePath("db")).thenReturn(database);
      when(access.resourcePath(eq("db"), anyString(), eq(database))).thenAnswer(invocation -> {
        final String resource = invocation.getArgument(1);
        final Path resolved = AccessControl.namedPath(windowsResources, resource, "resource");
        assertThat(requireNonNull(resolved.getFileName()).toString()).isEqualTo("orders");
        return resources.resolve(requireNonNull(resolved.getFileName()).toString());
      });
      final SnapshotRegistry snapshots = new SnapshotRegistry(access);
      snapshots.create("db", "Orders", "first", 3);
      for (final String alias : List.of("orders", "Orders", "ORDERS")) {
        assertThat(snapshots.resolve("db", alias, "first")).isEqualTo(3);
        assertThat(snapshots.list("db", alias)).isEqualTo(Map.of("first", 3));
      }
      snapshots.create("db", "orders", "second", 7);
      assertThat(snapshots.list("db", "Orders")).isEqualTo(Map.of("first", 3, "second", 7));
      assertThat(snapshots.delete("db", "ORDERS", "first")).isTrue();

      final SnapshotRegistry reopened = new SnapshotRegistry(access);
      for (final String alias : List.of("orders", "Orders", "ORDERS")) {
        assertThat(reopened.resolve("db", alias, "first")).isNull();
        assertThat(reopened.resolve("db", alias, "second")).isEqualTo(7);
        assertThat(reopened.list("db", alias)).isEqualTo(Map.of("second", 7));
      }
      assertThat(reopened.delete("db", "Orders", "second")).isTrue();
      assertThat(new SnapshotRegistry(access).list("db", "ORDERS")).isEmpty();
    }
  }

  @Test
  void resolveReturnsNullForUnknown() {
    assertThat(registry.resolve("db1", "res1", "nope")).isNull();
    assertThat(registry.resolve("nodb", "nores", "nope")).isNull();
  }

  @Test
  void deleteRemovesSnapshot() {
    registry.create("db1", "res1", "snap1", 1);
    assertThat(registry.delete("db1", "res1", "snap1")).isTrue();
    assertThat(registry.resolve("db1", "res1", "snap1")).isNull();
  }

  @Test
  void deleteReturnsFalseForNonexistent() {
    assertThat(registry.delete("db1", "res1", "nope")).isFalse();
  }

  @Test
  void listReturnsAllSnapshotsForResource() {
    registry.create("db1", "res1", "snap1", 1);
    registry.create("db1", "res1", "snap2", 2);
    registry.create("db1", "res2", "other", 3);

    var list = registry.list("db1", "res1");
    assertThat(list).hasSize(2);
    assertThat(list).containsEntry("snap1", 1);
    assertThat(list).containsEntry("snap2", 2);
  }

  @Test
  void listReturnsEmptyForUnknown() {
    assertThat(registry.list("nodb", "nores")).isEmpty();
  }

  @Test
  void persistAndLoadRoundTrip() throws Exception {
    // Create the database directory so persist can write to it
    Files.createDirectories(tempDir.resolve("mydb"));

    registry.create("mydb", "res1", "v1", 10);
    registry.create("mydb", "res1", "v2", 20);

    // Create a new registry and load from disk
    var registry2 = new SnapshotRegistry(new AccessControl(McpServerConfig.defaults(tempDir)));

    assertThat(registry2.resolve("mydb", "res1", "v1")).isEqualTo(10);
    assertThat(registry2.resolve("mydb", "res1", "v2")).isEqualTo(20);
  }

  @Test
  void persistIsAtomicViaTemporaryFile() throws Exception {
    Files.createDirectories(tempDir.resolve("mydb"));
    registry.create("mydb", "res1", "snap", 1);

    // After persist, no .tmp file should remain
    var tmpFile = tempDir.resolve("mydb").resolve(".sirix-mcp-snapshots.json.tmp");
    assertThat(tmpFile).doesNotExist();

    // But the main file should exist
    var mainFile = tempDir.resolve("mydb").resolve(".sirix-mcp-snapshots.json");
    assertThat(mainFile).exists();
  }

  // --- Name validation tests ---

  @Test
  void rejectsNullName() {
    assertThatThrownBy(() -> registry.create("db", "res", null, 1)).isInstanceOf(IllegalArgumentException.class)
                                                                   .hasMessageContaining("Invalid snapshot name");
  }

  @Test
  void rejectsEmptyName() {
    assertThatThrownBy(() -> registry.create("db", "res", "", 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsPathTraversalNames() {
    assertThatThrownBy(() -> registry.create("db", "res", "../etc/passwd", 1)).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.create("db", "res", "../../root", 1)).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void rejectsNamesStartingWithDot() {
    assertThatThrownBy(() -> registry.create("db", "res", ".hidden", 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNamesWithSlashes() {
    assertThatThrownBy(() -> registry.create("db", "res", "a/b", 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.create("db", "res", "a\\b", 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void acceptsValidNames() {
    registry.create("db", "res", "baseline-v1", 1);
    registry.create("db", "res", "before_migration", 2);
    registry.create("db", "res", "release.2024.01", 3);
    registry.create("db", "res", "A123", 4);
    assertThat(registry.list("db", "res")).hasSize(4);
  }

  @Test
  void rejectsTooLongName() {
    var longName = "a".repeat(129);
    assertThatThrownBy(() -> registry.create("db", "res", longName, 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedPersistedLabelsAreNotOverwrittenByASecondAttempt() throws IOException {
    final Path db = tempDir.resolve("broken");
    Files.createDirectories(db);
    final Path file = db.resolve(".sirix-mcp-snapshots.json");
    final String corrupt = "{broken";
    Files.writeString(file, corrupt);
    final var registry = new SnapshotRegistry(new AccessControl(McpServerConfig.defaults(tempDir)));
    assertThatThrownBy(() -> registry.create("broken", "res", "first", 1)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> registry.create("broken", "res", "second", 2)).isInstanceOf(RuntimeException.class);
    assertThat(Files.readString(file)).isEqualTo(corrupt);
  }

  @Test
  void persistenceFailuresRestoreThePreviousLabels() throws IOException {
    registry.create("db", "res", "baseline", 1);
    Files.createDirectory(tempDir.resolve("db/.sirix-mcp-snapshots.json.tmp"));
    assertThatThrownBy(() -> registry.create("db", "res", "new", 2)).isInstanceOf(RuntimeException.class);
    assertThat(registry.resolve("db", "res", "new")).isNull();
    assertThatThrownBy(() -> registry.create("db", "res", "baseline", 2)).isInstanceOf(RuntimeException.class);
    assertThat(registry.resolve("db", "res", "baseline")).isEqualTo(1);
    assertThatThrownBy(() -> registry.delete("db", "res", "baseline")).isInstanceOf(RuntimeException.class);
    assertThat(registry.resolve("db", "res", "baseline")).isEqualTo(1);
    assertThat(new SnapshotRegistry(new AccessControl(McpServerConfig.defaults(tempDir))).resolve("db", "res",
        "baseline")).isEqualTo(1);
  }
}
