package io.sirix.access;

import io.sirix.cache.BufferManager;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import java.nio.file.Path;

/**
 * A factory for resource managers.
 *
 * @author Joao Sousa
 */
public interface ResourceSessionFactory<R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>> {
  R create(ResourceConfiguration resourceConfig, BufferManager bufferManager, Path resourceFile);
}
