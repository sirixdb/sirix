package org.sirix.access;

import org.sirix.access.json.JsonResourceStore;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A database session factory for Json databases.
 *
 * @author Joao Sousa
 */
@Singleton
public class LocalJsonDatabaseFactory implements LocalDatabaseFactory<JsonResourceManager> {

    /**
     * Logger for {@link LocalJsonDatabaseFactory}.
     */
    private static final Logger logger = LoggerFactory.getLogger(LocalJsonDatabaseFactory.class);

    @Inject
    LocalJsonDatabaseFactory() {}

    @Override
    public Database<JsonResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {

        logger.trace("Creating new local json database");
        return new LocalJsonDatabase(configuration, new JsonResourceStore(user));
    }

}
