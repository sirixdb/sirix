package io.sirix.access.trx.page;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Factory and destroyer for pooled {@link StorageEngineReader} instances.
 *
 * <p>Provides a {@link Supplier} (create) and a {@link Consumer} (destroy) that
 * are wired into the {@link io.sirix.utils.ObjectPool} used by
 * {@link AbstractResourceSession}.
 */
public final class StorageEngineReaderFactory implements Supplier<StorageEngineReader>, Consumer<StorageEngineReader> {

  private final AbstractResourceSession<?, ?> resourceSession;

  public StorageEngineReaderFactory(final AbstractResourceSession<?, ?> resourceSession) {
    this.resourceSession = resourceSession;
  }

  /** Creates a new {@link StorageEngineReader} at the most recent revision. */
  @Override
  public StorageEngineReader get() {
    return resourceSession.createStorageEngineReader();
  }

  /** Destroys a pooled {@link StorageEngineReader} by closing it. */
  @Override
  public void accept(final StorageEngineReader storageEngineReader) {
    storageEngineReader.close();
  }
}
