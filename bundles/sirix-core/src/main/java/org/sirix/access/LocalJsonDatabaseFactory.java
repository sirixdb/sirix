package org.sirix.access;

import org.sirix.access.json.JsonResourceStore;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
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

    private final PathBasedPool<Database<?>> sessions;
    private final PathBasedPool<ResourceManager<?, ?>> resourceManagers;

    private final WriteLocksRegistry writeLocks;

    @Inject
    LocalJsonDatabaseFactory(final PathBasedPool<Database<?>> sessions,
                             final PathBasedPool<ResourceManager<?, ?>> resourceManagers,
                             final WriteLocksRegistry writeLocks) {
        this.sessions = sessions;
        this.writeLocks = writeLocks;
        this.resourceManagers = resourceManagers;
    }

    @Override
    public Database<JsonResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        checkNotNull(configuration);
        checkNotNull(user);

        logger.trace("Creating new local json database");

        return new LocalDatabase<>(
                configuration,
                sessions,
                new JsonResourceStore(user, writeLocks, resourceManagers),
                writeLocks,
                resourceManagers);
    }

}
