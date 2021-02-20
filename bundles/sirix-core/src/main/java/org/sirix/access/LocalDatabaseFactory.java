package org.sirix.access;

import org.sirix.api.Database;
import org.sirix.api.ResourceManager;

/**
 * A factory that creates {@link Database database sessions}.
 *
 * @author Joao Sousa
 */
public interface LocalDatabaseFactory<M extends ResourceManager<?, ?>> {

    Database<M> createDatabase(DatabaseConfiguration configuration, final User user);

}
