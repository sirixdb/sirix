package io.sirix.io;

import io.sirix.access.ResourceConfiguration;

/**
 * ServiceLoader-registered test provider that reports itself unavailable — models an enterprise
 * provider whose native library or license check fails (see {@link StorageProviderSpiTest}).
 */
public final class UnavailableTestStorageProvider implements StorageProvider {

  public static final String NAME = "UNAVAILABLE_TEST_STORAGE";

  public static final String REASON = "test provider is always unavailable";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isAvailable() {
    return false;
  }

  @Override
  public String getUnavailabilityReason() {
    return REASON;
  }

  @Override
  public IOStorage createStorage(final ResourceConfiguration resourceConfig) {
    throw new IllegalStateException(REASON);
  }
}
