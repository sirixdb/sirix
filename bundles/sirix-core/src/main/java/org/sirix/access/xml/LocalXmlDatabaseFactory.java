package org.sirix.access.xml;

import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.LocalDatabaseFactory;
import org.sirix.access.User;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
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
public class LocalXmlDatabaseFactory implements LocalDatabaseFactory<XmlResourceManager> {

    /**
     * Logger for {@link LocalXmlDatabaseFactory}.
     */
    private static final Logger logger = LoggerFactory.getLogger(LocalXmlDatabaseFactory.class);

    private final Provider<XmlLocalDatabaseComponent.Builder> subcomponentBuilder;

    @Inject
    LocalXmlDatabaseFactory(final Provider<XmlLocalDatabaseComponent.Builder> subcomponentBuilder) {

        this.subcomponentBuilder = subcomponentBuilder;
    }

    @Override
    public Database<XmlResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {
        logger.trace("Creating new local xml database");

        return this.subcomponentBuilder.get()
                .databaseConfiguration(configuration)
                .user(user)
                .build()
                .database();
    }
}
