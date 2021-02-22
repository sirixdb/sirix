package org.sirix.access.json;

import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.LocalDatabaseFactory;
import org.sirix.access.User;
import org.sirix.access.json.JsonLocalDatabaseComponent.Builder;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
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

    private final Provider<JsonLocalDatabaseComponent.Builder> subComponentBuilder;

    @Inject
    LocalJsonDatabaseFactory(final Provider<Builder> subComponentBuilder) {
        this.subComponentBuilder = subComponentBuilder;
    }

    @Override
    public Database<JsonResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        logger.trace("Creating new local json database");

        return this.subComponentBuilder.get()
                .databaseConfiguration(configuration)
                .user(user)
                .build()
                .database();
    }

}
