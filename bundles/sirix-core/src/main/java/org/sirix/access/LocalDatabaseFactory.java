package org.sirix.access;

import org.sirix.api.Database;
import org.sirix.api.ResourceSession;

/**
 * A factory that creates {@link Database database sessions}.
 *
 * @param <S> the resource session
 * @author Joao Sousa
 */
public interface LocalDatabaseFactory<S extends ResourceSession<?, ?>> {
  Database<S> createDatabase(DatabaseConfiguration configuration, final User user);
}
