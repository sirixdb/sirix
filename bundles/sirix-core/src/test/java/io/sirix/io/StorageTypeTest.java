package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.UnsupportedStoreFeatureException;
import io.sirix.page.StoreFeatures;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.util.Optional;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

/**
 * FMT-401: the storage factory must preserve proof ownership and feature refusals across SPI
 * dispatch.
 */
final class StorageTypeTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void builtInFactoryOpensTheVerifiedResource(final StorageType backend) {
    final ResourceConfiguration config = fixture("builtin", backend);
    final var proof = ResourceFormatCompatibility.verify(config);
    final IOStorage storage = StorageType.getStorage(config, proof);
    try {
      assertEquals(0, storage.getOpeningUberPage().getRevisionNumber());
      assertEquals(StoreFeatures.NONE, storage.getOpeningUberPage().getFeatures());
    } finally {
      storage.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  void providerReceivesVerifiedPageAndCannotBypassProofRefusals(final StorageType backend) throws IOException {
    final ResourceConfiguration config = fixture("provider", backend);
    final ResourceConfiguration other = fixture("other", backend);
    final var proof = ResourceFormatCompatibility.verify(config);
    final byte[] data = Files.readAllBytes(config.resourcePath.resolve("data").resolve(IOStorage.FILENAME));
    final byte[] revisions =
        Files.readAllBytes(config.resourcePath.resolve("data").resolve(IOStorage.REVISIONS_FILENAME));
    final IOStorage actual = backend.getInstance(config);
    try (final var providers = mockStatic(StorageProviders.class)) {
      // An external provider is an SPI boundary. It intentionally does not recheck the factory's
      // preconditions; a real backend above supplies the returned storage and observable page.
      final class Provider implements StorageProvider {
        int calls;
        @Nullable
        UberPage received;

        @Override
        public String getName() {
          return backend.name();
        }

        @Override
        public boolean isAvailable() {
          return true;
        }

        @Override
        public IOStorage createStorage(final ResourceConfiguration ignored) {
          calls++;
          return actual;
        }

        @Override
        public IOStorage createStorage(final ResourceConfiguration ignored, final UberPage page) {
          calls++;
          received = page;
          return actual;
        }
      }
      final Provider provider = new Provider();
      providers.when(() -> StorageProviders.get(backend.name())).thenReturn(Optional.of(provider));
      assertSame(actual, StorageType.getStorage(config, proof));
      assertEquals(0, Objects.requireNonNull(provider.received).getRevisionNumber());
      assertEquals(StoreFeatures.NONE, Objects.requireNonNull(provider.received).getFeatures());
      assertEquals(0, actual.getOpeningUberPage().getRevisionNumber());
      assertEquals(1, provider.calls);
      assertSame(actual, StorageType.getStorage(config));
      assertEquals(2, provider.calls);
      assertThrows(IllegalArgumentException.class, () -> StorageType.getStorage(other, proof));
      assertEquals(2, provider.calls, "wrong-resource proof must refuse before provider side effects");
      proof.uberPage().addFeatures(new StoreFeatures(1, 0, 0));
      assertThrows(UnsupportedStoreFeatureException.class, () -> StorageType.getStorage(config, proof));
      assertEquals(2, provider.calls, "unreadable features must refuse before provider side effects");
    } finally {
      actual.close();
    }
    assertArrayEquals(data, Files.readAllBytes(config.resourcePath.resolve("data").resolve(IOStorage.FILENAME)));
    assertArrayEquals(revisions,
        Files.readAllBytes(config.resourcePath.resolve("data").resolve(IOStorage.REVISIONS_FILENAME)));
  }

  private ResourceConfiguration fixture(final String name, final StorageType backend) {
    final Path database = directory.resolve(name);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(database)));
    try (final var opened = Databases.openJsonDatabase(database)) {
      assertTrue(opened.createResource(ResourceConfiguration.newBuilder("resource").storageType(backend).build()));
    }
    Databases.clearGlobalCaches();
    return ResourceConfiguration.deserialize(database.resolve("resources").resolve("resource"));
  }
}
