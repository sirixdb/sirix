package io.sirix.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.io.ResourceFormatCompatibility;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The legacy SPI overloads must still return usable sessions when given a compatibility proof. */
class ResourceSessionFactoryTest {
  @TempDir
  Path directory;

  @Test
  void compatibilityProofDefaultsPreserveSessionAndStoreContracts() {
    final Path path = directory.resolve("legacy");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource").build());
      final ResourceConfiguration configuration;
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        configuration = session.getResourceConfig();
        writer.insertStringValueAsFirstChild("persisted");
        writer.commit();
      }
      final BufferManager buffers = Databases.getGlobalBufferManager();
      final Path resource = configuration.resourcePath;
      final ResourceSessionFactory<JsonResourceSession> legacyFactory = (config, manager, location) -> {
        assertSame(configuration, config);
        assertSame(buffers, manager);
        assertEquals(resource, location);
        return database.beginResourceSession("resource");
      };
      try (final var session =
          legacyFactory.create(configuration, buffers, resource, ResourceFormatCompatibility.verify(configuration))) {
        assertPersistedValue(session);
      }
      final var delegate = new ResourceStoreImpl<>(new PathBasedPool<>(), legacyFactory);
      final ResourceStore<JsonResourceSession> legacyStore = new ResourceStore<>() {
        @Override
        public DatabaseNotifications getNotifications() {
          return delegate.getNotifications();
        }

        @Override
        public void assertCanClose() {
          delegate.assertCanClose();
        }

        @Override
        public JsonResourceSession beginResourceSession(final ResourceConfiguration config, final BufferManager manager,
            final Path location) {
          return delegate.beginResourceSession(config, manager, location);
        }

        @Override
        public boolean hasOpenResourceSession(final Path location) {
          return delegate.hasOpenResourceSession(location);
        }

        @Override
        public JsonResourceSession getOpenResourceSession(final Path location) {
          return delegate.getOpenResourceSession(location);
        }

        @Override
        public boolean closeResourceSession(final Path location) {
          return delegate.closeResourceSession(location);
        }

        @Override
        public void close() {
          delegate.close();
        }
      };
      try (legacyStore;
          final var session = legacyStore.beginResourceSession(configuration, buffers, resource,
              ResourceFormatCompatibility.verify(configuration))) {
        assertTrue(legacyStore.hasOpenResourceSession(resource));
        assertSame(session, legacyStore.getOpenResourceSession(resource));
        assertPersistedValue(session);
      }
    }
  }

  private static void assertPersistedValue(final JsonResourceSession session) {
    try (final var reader = session.beginNodeReadOnlyTrx()) {
      assertEquals(1, reader.getRevisionNumber());
      assertTrue(reader.moveToFirstChild());
      assertEquals("persisted", reader.getValue());
    }
  }
}
