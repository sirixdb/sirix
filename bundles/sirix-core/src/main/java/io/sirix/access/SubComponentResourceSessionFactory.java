package io.sirix.access;

import io.sirix.cache.BufferManager;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import javax.inject.Provider;
import java.nio.file.Path;

/**
 * A parameterizable resource session factory that creates {@link ResourceSession} instances based
 * on {@link GenericResourceSessionComponent a resource session subcomponent}.
 *
 * @author Joao Sousa
 */
public class SubComponentResourceSessionFactory<B extends GenericResourceSessionComponent.Builder<B, R, ?>, R extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceSessionFactory<R> {

  private final Provider<B> subComponentBuilder;

  public SubComponentResourceSessionFactory(final Provider<B> subComponentBuilder) {
    this.subComponentBuilder = subComponentBuilder;
  }

  @Override
  public R create(final ResourceConfiguration resourceConfig, final BufferManager bufferManager,
      final Path resourceFile) {
    return this.subComponentBuilder.get()
                                   .resourceConfig(resourceConfig)
                                   .bufferManager(bufferManager)
                                   .resourceFile(resourceFile)
                                   .build()
                                   .resourceSession();
  }
}
