package org.sirix.access;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.sirix.access.xml.XmlResourceStore;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A database session factory for Json databases.
 *
 * @author Joao Sousa
 */
@Singleton
public class LocalXmlDatabaseFactory implements LocalDatabaseFactory<XmlResourceManager> {

    /**
     * Logger for {@link LocalXmlDatabaseFactory}.
     */
    private static final Logger logger = LoggerFactory.getLogger(LocalXmlDatabaseFactory.class);

    private final PathBasedPool<Database<?>> sessions;
    private final PathBasedPool<ResourceManager<?, ?>> resourceManagers;

    @Inject
    LocalXmlDatabaseFactory(final PathBasedPool<Database<?>> sessions,
                            final PathBasedPool<ResourceManager<?, ?>> resourceManagers) {
        this.sessions = sessions;
        this.resourceManagers = resourceManagers;
    }

    @Override
    public Database<XmlResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        logger.trace("Creating new local xml database");

        return new LocalDatabase<>(
                configuration,
                this.sessions,
                new XmlResourceStore(user, resourceManagers),
                resourceManagers
        );
    }
}
