package io.sirix.io;

import io.sirix.access.ResourceConfiguration;
import io.sirix.io.bytepipe.ByteHandler;

/**
 * ServiceLoader-registered test provider for {@link StorageProviderSpiTest}. The name matches no
 * {@link StorageType} constant on purpose: it must never be dispatched for real resources.
 */
public final class TestStorageProvider implements StorageProvider {

  public static final String NAME = "TEST_STORAGE";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public IOStorage createStorage(final ResourceConfiguration resourceConfig) {
    return new DummyStorage();
  }

  /** Inert storage — only identity matters to the tests. */
  public static final class DummyStorage implements IOStorage {

    @Override
    public Reader createReader() {
      throw new UnsupportedOperationException("test-only storage");
    }

    @Override
    public Writer createWriter() {
      throw new UnsupportedOperationException("test-only storage");
    }

    @Override
    public void close() {
      // no-op
    }

    @Override
    public boolean exists() {
      return false;
    }

    @Override
    public ByteHandler getByteHandler() {
      throw new UnsupportedOperationException("test-only storage");
    }

    @Override
    public RevisionIndexHolder getRevisionIndexHolder() {
      throw new UnsupportedOperationException("test-only storage");
    }
  }
}
