package io.sirix.mcp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotRegistryTest {

  @TempDir
  Path tempDir;

  private SnapshotRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new SnapshotRegistry(tempDir);
  }

  @Test
  void createAndResolveSnapshot() {
    registry.create("db1", "res1", "baseline", 5);
    assertThat(registry.resolve("db1", "res1", "baseline")).isEqualTo(5);
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
    var registry2 = new SnapshotRegistry(tempDir);
    registry2.loadForDatabase("mydb");

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
    assertThatThrownBy(() -> registry.create("db", "res", null, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid snapshot name");
  }

  @Test
  void rejectsEmptyName() {
    assertThatThrownBy(() -> registry.create("db", "res", "", 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsPathTraversalNames() {
    assertThatThrownBy(() -> registry.create("db", "res", "../etc/passwd", 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.create("db", "res", "../../root", 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNamesStartingWithDot() {
    assertThatThrownBy(() -> registry.create("db", "res", ".hidden", 1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsNamesWithSlashes() {
    assertThatThrownBy(() -> registry.create("db", "res", "a/b", 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.create("db", "res", "a\\b", 1))
        .isInstanceOf(IllegalArgumentException.class);
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
    assertThatThrownBy(() -> registry.create("db", "res", longName, 1))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
