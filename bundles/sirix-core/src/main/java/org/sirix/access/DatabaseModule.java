package org.sirix.access;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import org.sirix.access.json.LocalJsonDatabaseFactory;
import org.sirix.access.xml.LocalXmlDatabaseFactory;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.xml.XmlResourceManager;

import javax.inject.Singleton;

/**
 * The DI module that bridges interfaces to implementations.
 *
 * @author Joao Sousa
 */
@Module
public interface DatabaseModule {

    @Binds
    @Singleton
    LocalDatabaseFactory<JsonResourceManager> bindJsonDatabaseFactory(LocalJsonDatabaseFactory jsonFactory);

    @Binds
    @Singleton
    LocalDatabaseFactory<XmlResourceManager> bindXmlDatabaseFactory(LocalXmlDatabaseFactory xmlFactory);

    @Provides
    @Singleton
    static PathBasedPool<Database<?>> databaseSessions() {
        return new PathBasedPool<>();
    }

    @Provides
    @Singleton
    static PathBasedPool<ResourceManager<?, ?>> resourceManagers() {
        return new PathBasedPool<>();
    }



}
