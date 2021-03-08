package org.sirix.access;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;

import javax.inject.Provider;
import java.nio.file.Path;

/**
 * A parameterizable resource manager factory that creates {@link ResourceManager} instances based on
 * {@link GenericResourceManagerComponent a resource manager subcomponent}.
 *
 * @author Joao Sousa
 */
public class SubComponentResourceManagerFactory<B extends GenericResourceManagerComponent.Builder<B, R, ?>,
        R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
        implements ResourceManagerFactory<R> {

    private final Provider<B> subComponentBuilder;

    public SubComponentResourceManagerFactory(final Provider<B> subComponentBuilder) {
        this.subComponentBuilder = subComponentBuilder;
    }

    @Override
    public R create(final ResourceConfiguration resourceConfig,
                    final BufferManager bufferManager,
                    final Path resourceFile) {

        return this.subComponentBuilder.get()
                .resourceConfig(resourceConfig)
                .bufferManager(bufferManager)
                .resourceFile(resourceFile)
                .build()
                .resourceManager();
    }
}
