package org.sirix.access;

import org.sirix.access.json.JsonResourceStore;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A database session factory for Json databases.
 *
 * @author Joao Sousa
 */
@Singleton
public class LocalJsonDatabaseFactory implements LocalDatabaseFactory<JsonResourceManager> {

    @Inject
    LocalJsonDatabaseFactory() {}

    @Override
    public Database<JsonResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {

        return new LocalJsonDatabase(configuration, new JsonResourceStore(user));
    }

}
