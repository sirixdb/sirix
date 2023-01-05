package org.sirix.access;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.cache.BufferManager;

import javax.inject.Provider;
import java.nio.file.Path;

/**
 * A parameterizable resource session factory that creates {@link ResourceSession} instances based on
 * {@link GenericResourceSessionComponent a resource session subcomponent}.
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
                                   .resourceManager();
  }
}
