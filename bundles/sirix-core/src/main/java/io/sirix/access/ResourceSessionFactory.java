package io.sirix.access;

import io.sirix.io.ResourceFormatCompatibility.Proof;

import org.jspecify.annotations.Nullable;
import io.sirix.cache.BufferManager;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import java.nio.file.Path;

/**
 * A factory for resource sessions.
 *
 * @author Joao Sousa
 */
public interface ResourceSessionFactory<R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> {
  R create(ResourceConfiguration resourceConfig, BufferManager bufferManager, Path resourceFile);

  default R create(final ResourceConfiguration resourceConfig, final BufferManager bufferManager,
      final Path resourcePath, final @Nullable Proof compatibility) {
    return create(resourceConfig, bufferManager, resourcePath);
  }
}
