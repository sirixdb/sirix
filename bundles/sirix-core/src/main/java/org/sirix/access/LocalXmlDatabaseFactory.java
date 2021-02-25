package org.sirix.access;

import org.sirix.access.xml.XmlResourceStore;
import org.sirix.api.Database;
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

    private final DatabaseSessionPool sessions;

    @Inject
    LocalXmlDatabaseFactory(final DatabaseSessionPool sessions) {
        this.sessions = sessions;
    }

    @Override
    public Database<XmlResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        logger.trace("Creating new local xml database");

        return new LocalDatabase<>(configuration, this.sessions, new XmlResourceStore(user));
    }
}
