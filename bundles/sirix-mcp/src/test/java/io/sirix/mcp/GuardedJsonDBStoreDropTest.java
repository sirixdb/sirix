package io.sirix.mcp;

import io.sirix.access.Databases;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GuardedJsonDBStoreDropTest {
  @TempDir
  Path directory;

  @Test
  void boundDropChecksLogicalAllowListAndKeepsItsPhysicalTarget() {
    boundDrop(false, List.of("orders"), true);
  }

  @Test
  void boundDropStillRejectsReadOnlyAccess() {
    boundDrop(true, List.of("orders"), false);
  }

  @Test
  void boundDropStillRejectsDisallowedDatabaseNames() {
    boundDrop(false, List.of("other"), false);
  }

  private void boundDrop(final boolean readOnly, final List<String> allowed, final boolean canDelete) {
    final Path storePath = directory.resolve("store");
    final Path targetParent = directory.resolve("target");
    final Path localDatabase = storePath.resolve("orders");
    final Path targetDatabase = targetParent.resolve("orders");
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storePath).build();
        final BasicJsonDBStore target = BasicJsonDBStore.newBuilder().location(targetParent).build()) {
      store.create("orders", "resource1", "[\"local\"]").close();
      target.create("orders", "resource1", "[\"target\"]").close();
      final var config = new McpServerConfig("test", "1.0.0", "stdio", storePath.toString(), readOnly, allowed,
          List.of(), Map.of(), 100, 4096, true, true, false, null);
      final var guarded = new GuardedJsonDBStore(store, new AccessControl(config));
      if (canDelete) {
        guarded.drop("orders", targetDatabase);
      } else {
        assertThrows(AccessControl.AccessDeniedException.class, () -> guarded.drop("orders", targetDatabase));
      }
      assertEquals(!canDelete, Files.exists(targetDatabase));
      assertTrue(Files.exists(localDatabase));
      try (final var session = requireNonNull(store.lookup("orders")).getDatabase().beginResourceSession("resource1");
          final var reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertTrue(reader.moveToFirstChild());
        assertEquals("local", reader.getValue());
      }
    } finally {
      Databases.removeDatabase(localDatabase);
      Databases.removeDatabase(targetDatabase);
    }
  }
}
