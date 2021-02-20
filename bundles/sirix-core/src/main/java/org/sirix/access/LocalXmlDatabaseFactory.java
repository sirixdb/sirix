package org.sirix.access;

import org.sirix.access.xml.XmlResourceStore;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * A database session factory for Json databases.
 *
 * @author Joao Sousa
 */
@Singleton
public class LocalXmlDatabaseFactory implements LocalDatabaseFactory<XmlResourceManager> {

    @Inject
    LocalXmlDatabaseFactory() {}

    @Override
    public Database<XmlResourceManager> createDatabase(final DatabaseConfiguration configuration, final User user) {

        return new LocalXmlDatabase(configuration, new XmlResourceStore(user));
    }
}
