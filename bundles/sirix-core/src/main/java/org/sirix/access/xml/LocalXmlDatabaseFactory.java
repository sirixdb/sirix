package org.sirix.access.xml;

import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.LocalDatabase;
import org.sirix.access.LocalDatabaseFactory;
import org.sirix.access.PathBasedPool;
import org.sirix.access.User;
import org.sirix.access.WriteLocksRegistry;
import org.sirix.access.trx.page.PageTrxFactory;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlResourceManager;
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
public class LocalXmlDatabaseFactory implements LocalDatabaseFactory<XmlResourceManager> {

    /**
     * Logger for {@link LocalXmlDatabaseFactory}.
     */
    private static final Logger logger = LoggerFactory.getLogger(LocalXmlDatabaseFactory.class);

    private final PathBasedPool<Database<?>> sessions;
    private final PathBasedPool<ResourceManager<?, ?>> resourceManagers;
    private final WriteLocksRegistry writeLocks;

    @Inject
    LocalXmlDatabaseFactory(final PathBasedPool<Database<?>> sessions,
                            final PathBasedPool<ResourceManager<?, ?>> resourceManagers,
                            final WriteLocksRegistry writeLocks) {
        this.sessions = sessions;
        this.resourceManagers = resourceManagers;
        this.writeLocks = writeLocks;
    }

    @Override
    public Database<XmlResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        logger.trace("Creating new local xml database");

        final String databaseName = configuration.getDatabaseName();
        final PageTrxFactory pageTrxFactory = new PageTrxFactory( configuration.getDatabaseType());
        return new LocalDatabase<>(
                configuration,
                this.sessions,
                new XmlResourceStore(user, writeLocks, resourceManagers, databaseName, pageTrxFactory),
                writeLocks,
                resourceManagers
        );
    }
}
