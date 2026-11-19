package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class HOTCommitWorkspaceTest {
  @TempDir
  Path directory;

  @Test
  void lazyWorkspaceIsSessionScopedAndReleasedAtClose() throws ReflectiveOperationException {
    final Field field = AbstractResourceSession.class.getDeclaredField("hotCommitScratch");
    field.setAccessible(true);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("db"))));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.resolve("db"))) {
      database.createResource(ResourceConfiguration.newBuilder("a").storeDiffs(false).build());
      database.createResource(ResourceConfiguration.newBuilder("b").storeDiffs(false).build());
      final JsonResourceSession first = database.beginResourceSession("a");
      final InternalResourceSession<?, ?> internal = (InternalResourceSession<?, ?>) first;
      try (first; JsonResourceSession second = database.beginResourceSession("b")) {
        assertNull(field.get(first), "read-only sessions must not allocate normalization workspace");
        try (JsonNodeTrx trx = first.beginNodeTrx()) {
          final StorageEngineWriter original = trx.getStorageEngineWriter();
          trx.commit();
          assertNull(field.get(first), "no-HOT commits must not initialize the workspace");
          final HOTCommitNormalizer.Scratch scratch = internal.getHOTCommitScratch();
          assertNotSame(scratch, ((InternalResourceSession<?, ?>) second).getHOTCommitScratch());
          assertSame(scratch, internal.getHOTCommitScratch());
          trx.commit();
          assertNotSame(original, trx.getStorageEngineWriter());
          assertSame(scratch, internal.getHOTCommitScratch());
          trx.rollback();
          assertSame(scratch, internal.getHOTCommitScratch());
        }
        try (JsonNodeTrx trx = first.beginNodeTrx()) {
          assertSame(field.get(first), internal.getHOTCommitScratch());
          trx.rollback();
        }
      }
      assertNull(field.get(first), "close must release the bounded workspace");
      assertThrows(IllegalStateException.class, internal::getHOTCommitScratch);
    } finally {
      Databases.clearGlobalCaches();
    }
  }
}
