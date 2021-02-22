package org.sirix.access;

import dagger.Component;
import org.sirix.access.json.JsonLocalDatabaseComponent;
import org.sirix.access.xml.XmlLocalDatabaseComponent;
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

    JsonLocalDatabaseComponent.Builder jsonDatabaseBuilder();

    XmlLocalDatabaseComponent.Builder xmlDatabaseBuilder();

    LocalDatabaseFactory<JsonResourceManager> jsonDatabaseFactory();

    LocalDatabaseFactory<XmlResourceManager> xmlDatabaseFactory();

    PathBasedPool<Database<?>> sessions();

}
