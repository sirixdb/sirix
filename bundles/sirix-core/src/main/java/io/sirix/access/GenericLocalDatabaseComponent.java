package io.sirix.access;

import dagger.BindsInstance;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;

/**
 * An interface that aggregates all the common logic between {@link Database} subcomponents.
 *
 * @author Joao Sousa
 */
public interface GenericLocalDatabaseComponent<R extends ResourceSession<?, ?>, C extends GenericResourceSessionComponent.Builder<C, R, ?>> {

  Database<R> database();

  C resourceManagerBuilder();

  interface Builder<B extends Builder<B>> {

    @BindsInstance
    B databaseConfiguration(DatabaseConfiguration configuration);

    @BindsInstance
    B user(User user);

    GenericLocalDatabaseComponent build();
  }
}
