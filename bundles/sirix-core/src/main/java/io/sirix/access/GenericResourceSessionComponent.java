package io.sirix.access;

import dagger.BindsInstance;
import io.sirix.cache.BufferManager;
import io.sirix.api.ResourceSession;

import java.nio.file.Path;

/**
 * An interface that aggregates all the common logic between {@link ResourceSession} subcomponents.
 *
 * @author Joao Sousa
 */
public interface GenericResourceSessionComponent<R extends ResourceSession<?, ?>> {

  R resourceManager();

  interface Builder<B extends GenericResourceSessionComponent.Builder<B, R, C>, R extends ResourceSession<?, ?>, C extends GenericResourceSessionComponent<R>> {

    @BindsInstance
    B resourceConfig(ResourceConfiguration resourceConfiguration);

    @BindsInstance
    B bufferManager(BufferManager bufferManager);

    @BindsInstance
    B resourceFile(Path resourceFile);

    C build();

  }
}
