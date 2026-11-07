package io.sirix.query;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MutationPreparationTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void failedPreparationUnwindsOwnedResourcesAndSessions(final boolean xml, final boolean failOpening) {
    final Path path = directory.resolve("database");
    final DatabaseConfiguration configuration = new DatabaseConfiguration(path);
    if (xml) {
      Databases.createXmlDatabase(configuration);
      try (final var database = Databases.openXmlDatabase(path)) {
        verifyPreparationRollback(database, failOpening);
      }
    } else {
      Databases.createJsonDatabase(configuration);
      try (final var database = Databases.openJsonDatabase(path)) {
        verifyPreparationRollback(database, failOpening);
      }
    }
  }

  private static <T extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> void verifyPreparationRollback(
      final Database<T> database, final boolean failOpening) {
    assertTrue(database.createResource(config("borrowed")));
    assertTrue(database.createResource(config("cold")));
    try (final T borrowed = database.beginResourceSession("borrowed");
        final NodeTrx writer = borrowed.beginNodeTrx()) {
      final List<ResourceConfiguration> configurations =
          List.of(config("borrowed"), config("cold"), config("fresh"), config("failed"));
      final SirixIOException injected = new SirixIOException(new IOException("preparation failed"));
      final Database<T> failing = failPreparation(database, failOpening, injected);

      assertSame(injected, assertThrows(SirixIOException.class,
          () -> MutationAuthorization.prepareResources(failing, "database", configurations)));
      assertEquals(0, injected.getSuppressed().length);
      assertFalse(borrowed.isClosed());
      assertFalse(writer.isClosed());
      assertSame(borrowed, database.getOpenResourceSession("borrowed"));
      assertNull(database.getOpenResourceSession("cold"));
      assertTrue(database.existsResource("cold"));
      assertFalse(database.existsResource("fresh"));
      assertFalse(database.existsResource("failed"));
      assertEquals(2, database.listResources().size());
      assertEquals(0, borrowed.getMostRecentRevisionNumber());
      database.removeResource("cold");

      final List<T> prepared = MutationAuthorization.prepareResources(database, "database", configurations);
      assertSame(borrowed, prepared.getFirst());
      for (int i = 1; i < prepared.size(); i++) {
        try (final T session = prepared.get(i); final NodeTrx transaction = session.beginNodeTrx()) {
          transaction.commit();
          assertEquals(1, session.getMostRecentRevisionNumber());
        }
      }
      writer.commit();
      assertEquals(1, borrowed.getMostRecentRevisionNumber());
      assertEquals(4, database.listResources().size());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> Database<T> failPreparation(
      final Database<T> delegate, final boolean failOpening, final SirixIOException injected) {
    return (Database<T>) Proxy.newProxyInstance(Database.class.getClassLoader(), new Class<?>[] {Database.class},
        (proxy, method, args) -> {
          if ((failOpening && method.getName().equals("beginResourceSession") && args[0].equals("failed"))
              || (!failOpening && method.getName().equals("createResource")
                  && ((ResourceConfiguration) args[0]).getName().equals("failed"))) {
            throw injected;
          }
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException failure) {
            throw failure.getCause();
          }
        });
  }

  private static ResourceConfiguration config(final String name) {
    return ResourceConfiguration.newBuilder(name).storageType(StorageType.FILE_CHANNEL).build();
  }
}
