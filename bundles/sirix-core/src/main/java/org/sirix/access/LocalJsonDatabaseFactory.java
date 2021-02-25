package org.sirix.access;

import org.sirix.access.json.JsonResourceStore;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

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

    private final DatabaseSessionPool sessions;

    @Inject
    LocalJsonDatabaseFactory(final DatabaseSessionPool sessions) {
        this.sessions = sessions;
    }

    @Override
    public Database<JsonResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        checkNotNull(configuration);
        checkNotNull(user);

        logger.trace("Creating new local json database");

        return new LocalDatabase<>(configuration, this.sessions, new JsonResourceStore(user));
    }

}
