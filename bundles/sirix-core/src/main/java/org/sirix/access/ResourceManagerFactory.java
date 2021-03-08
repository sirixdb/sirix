package org.sirix.access;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;

import java.nio.file.Path;

/**
 * A factory for resource managers.
 *
 * @author Joao Sousa
 */
public interface ResourceManagerFactory<R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>> {

    R create(ResourceConfiguration resourceConfig,
             BufferManager bufferManager,
             Path resourceFile);

}
