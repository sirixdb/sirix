package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link StorageProvider} SPI — the integration surface the sirix-enterprise
 * modules (io_uring, S3) plug into.
 *
 * <p>Two test providers are registered via {@code META-INF/services/io.sirix.io.StorageProvider}:
 * {@code TEST_STORAGE} (available) and {@code UNAVAILABLE_TEST_STORAGE} (unavailable). Their
 * names deliberately match no {@link StorageType} constant so they never interfere with other
 * tests' storage dispatch.
 *
 * <p>Regression coverage: {@link StorageType#fromString} used to map any registered external
 * provider name to a {@code FILE_CHANNEL} "marker" (referencing a dispatch method that never
 * existed). The marker was persisted into the resource configuration, so a resource configured
 * for an enterprise provider silently used the built-in FileChannelStorage on every open — the
 * requested backend was never selected and no error surfaced.
 */
public final class StorageProviderSpiTest {

  @Test
  public void builtInTypesResolveCaseInsensitively() {
    assertSame(StorageType.FILE_CHANNEL, StorageType.fromString("file_channel"));
    assertSame(StorageType.MEMORY_MAPPED, StorageType.fromString("Memory_Mapped"));
    assertSame(StorageType.S3, StorageType.fromString("s3"));
  }

  @Test
  public void unknownNameFailsWithAvailableAlternatives() {
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> StorageType.fromString("BOGUS_BACKEND"));
    assertTrue(e.getMessage(), e.getMessage().contains("No storage type or provider"));
  }

  @Test
  public void freeFormProviderNameFailsFastInsteadOfSilentlyBecomingFileChannel() {
    // The provider IS registered and available…
    assertTrue(StorageProviders.isAvailable(TestStorageProvider.NAME));
    // …but it cannot be carried by a resource configuration, so selecting it by name must fail
    // loudly rather than silently persisting FILE_CHANNEL (the pre-fix behavior).
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> StorageType.fromString(TestStorageProvider.NAME));
    assertTrue(e.getMessage(), e.getMessage().contains("cannot be selected by free-form name"));
  }

  @Test
  public void providerLookupIsCaseInsensitiveWithRootLocale() {
    final Optional<StorageProvider> lower = StorageProviders.get("test_storage");
    assertTrue(lower.isPresent());
    assertEquals(TestStorageProvider.NAME, lower.get().getName());
  }

  @Test
  public void createStorageDispatchesToAvailableProvider() {
    final IOStorage storage = StorageProviders.createStorage(TestStorageProvider.NAME, null);
    assertNotNull(storage);
    assertTrue(storage instanceof TestStorageProvider.DummyStorage);
  }

  @Test
  public void createStorageRejectsUnavailableProviderWithReason() {
    final IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> StorageProviders.createStorage(UnavailableTestStorageProvider.NAME, null));
    assertTrue(e.getMessage(), e.getMessage().contains(UnavailableTestStorageProvider.REASON));
  }

  @Test
  public void isExternalProviderDistinguishesBuiltInsFromProviders() {
    assertFalse(StorageType.isExternalProvider("FILE_CHANNEL"));
    assertTrue(StorageType.isExternalProvider(TestStorageProvider.NAME));
    assertFalse(StorageType.isExternalProvider("BOGUS_BACKEND"));
  }

  @Test
  public void unavailableProviderIsRegisteredButNotAvailable() {
    assertTrue(StorageProviders.get(UnavailableTestStorageProvider.NAME).isPresent());
    assertFalse(StorageProviders.isAvailable(UnavailableTestStorageProvider.NAME));
    assertFalse(StorageProviders.getAvailableProviderNames().contains(UnavailableTestStorageProvider.NAME));
    assertTrue(StorageProviders.getProviderNames().contains(UnavailableTestStorageProvider.NAME));
  }

  /** Compile-time-only reference so the config parameter type stays honest. */
  @SuppressWarnings("unused")
  private static void signatureCheck(final ResourceConfiguration config) {
    // no-op
  }
}
