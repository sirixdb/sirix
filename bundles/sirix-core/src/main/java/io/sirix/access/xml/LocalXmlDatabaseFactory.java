package io.sirix.access.xml;

import io.sirix.access.LocalDatabaseFactory;
import io.sirix.access.User;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlResourceSession;
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
public class LocalXmlDatabaseFactory implements LocalDatabaseFactory<XmlResourceSession> {

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
    public Database<XmlResourceSession> createDatabase(final DatabaseConfiguration configuration, final User user) {
        logger.trace("Creating new local XML database instance (open)");

        return this.subcomponentBuilder.get()
                .databaseConfiguration(configuration)
                .user(user)
                .build()
                .database();
    }
}
