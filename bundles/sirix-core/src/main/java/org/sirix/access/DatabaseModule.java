package org.sirix.access;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import org.sirix.access.json.JsonLocalDatabaseComponent;
import org.sirix.access.json.LocalJsonDatabaseFactory;
import org.sirix.access.xml.LocalXmlDatabaseFactory;
import org.sirix.api.Database;
import org.sirix.api.ResourceSession;
import org.sirix.access.xml.XmlLocalDatabaseComponent;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.api.xml.XmlResourceSession;

import javax.inject.Singleton;

/**
 * The DI module that bridges interfaces to implementations.
 *
 * @author Joao Sousa
 */
@Module(subcomponents = { JsonLocalDatabaseComponent.class, XmlLocalDatabaseComponent.class})
public interface DatabaseModule {

    @Binds
    @Singleton
    LocalDatabaseFactory<JsonResourceSession> bindJsonDatabaseFactory(LocalJsonDatabaseFactory jsonFactory);

    @Binds
    @Singleton
    LocalDatabaseFactory<XmlResourceSession> bindXmlDatabaseFactory(LocalXmlDatabaseFactory xmlFactory);

    @Provides
    @Singleton
    static PathBasedPool<Database<?>> databaseSessions() {
        return new PathBasedPool<>();
    }

    @Provides
    @Singleton
    static PathBasedPool<ResourceSession<?, ?>> resourceManagers() {
        return new PathBasedPool<>();
    }



}
