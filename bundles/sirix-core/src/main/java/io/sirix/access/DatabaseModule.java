package io.sirix.access;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.sirix.access.json.JsonLocalDatabaseComponent;
import io.sirix.access.json.LocalJsonDatabaseFactory;
import io.sirix.access.xml.LocalXmlDatabaseFactory;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.access.xml.XmlLocalDatabaseComponent;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;

import javax.inject.Singleton;

/**
 * The DI module that bridges interfaces to implementations.
 *
 * @author Joao Sousa
 */
@Module(subcomponents = {JsonLocalDatabaseComponent.class, XmlLocalDatabaseComponent.class})
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
  static PathBasedPool<ResourceSession<?, ?>> resourceSessions() {
    return new PathBasedPool<>();
  }



}
