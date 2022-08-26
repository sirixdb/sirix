package org.sirix.access.json;

import dagger.Module;
import dagger.Provides;
import org.sirix.access.*;
import org.sirix.api.Database;
import org.sirix.api.ResourceSession;
import org.sirix.api.TransactionManager;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.dagger.DatabaseScope;

import javax.inject.Provider;

/**
 * The module for {@link JsonLocalDatabaseComponent}.
 *
 * @author Joao Sousa
 */
@Module(includes = LocalDatabaseModule.class)
public interface JsonLocalDatabaseModule {

  @DatabaseScope
  @Provides
  static ResourceSessionFactory<JsonResourceSession> resourceManagerFactory(
      final Provider<JsonResourceSessionComponent.Builder> subComponentBuilder) {
    return new SubComponentResourceSessionFactory<>(subComponentBuilder);
  }

  @DatabaseScope
  @Provides
  static ResourceStore<JsonResourceSession> jsonResourceManager(
      final PathBasedPool<ResourceSession<?, ?>> allResourceManagers,
      final ResourceSessionFactory<JsonResourceSession> resourceSessionFactory) {
    return new ResourceStoreImpl<>(allResourceManagers, resourceSessionFactory);
  }

  @DatabaseScope
  @Provides
  static Database<JsonResourceSession> jsonDatabase(final TransactionManager transactionManager,
      final DatabaseConfiguration dbConfig, final PathBasedPool<Database<?>> sessions,
      final ResourceStore<JsonResourceSession> resourceStore, final WriteLocksRegistry writeLocks,
      final PathBasedPool<ResourceSession<?, ?>> resourceManagers) {
    return new LocalDatabase<>(transactionManager,
                               dbConfig,
                               sessions,
                               resourceStore,
                               writeLocks,
                               resourceManagers);
  }
}
