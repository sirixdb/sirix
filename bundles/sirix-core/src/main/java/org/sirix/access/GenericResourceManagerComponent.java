package org.sirix.access;

import dagger.BindsInstance;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;

import java.nio.file.Path;

/**
 * An interface that aggregates all the common logic between {@link ResourceManager} subcomponents.
 *
 * @author Joao Sousa
 */
public interface GenericResourceManagerComponent<R extends ResourceManager<?, ?>> {

    R resourceManager();

    interface Builder<B extends GenericResourceManagerComponent.Builder<B, R, C>,
            R extends ResourceManager<?, ?>,
            C extends GenericResourceManagerComponent<R>> {

        @BindsInstance
        B resourceConfig(ResourceConfiguration resourceConfiguration);

        @BindsInstance
        B bufferManager(BufferManager bufferManager);

        @BindsInstance
        B resourceFile(Path resourceFile);

        C build();

    }
}
