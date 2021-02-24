package org.sirix.access;

import dagger.Component;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.xml.XmlResourceManager;

import javax.inject.Singleton;

/**
 * The Dagger component that manages database dependencies. This class is internal and managed by {@link Databases}.
 *
 * @author Joao Sousa
 */
@Component(modules = DatabaseModule.class)
@Singleton
public interface DatabaseManager {

    LocalDatabaseFactory<JsonResourceManager> jsonDatabaseFactory();

    LocalDatabaseFactory<XmlResourceManager> xmlDatabaseFactory();

    DatabaseSessionPool sessions();

}
