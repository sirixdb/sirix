package io.sirix.access.json;

import dagger.Module;
import dagger.Provides;
import io.sirix.access.*;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.TransactionManager;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.dagger.DatabaseScope;

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
  static ResourceSessionFactory<JsonResourceSession> resourceSessionFactory(
      final Provider<JsonResourceSessionComponent.Builder> subComponentBuilder) {
    return new SubComponentResourceSessionFactory<>(subComponentBuilder);
  }

  @DatabaseScope
  @Provides
  static ResourceStore<JsonResourceSession> jsonResourceSession(
      final PathBasedPool<ResourceSession<?, ?>> allResourceSessions,
      final ResourceSessionFactory<JsonResourceSession> resourceSessionFactory) {
    return new ResourceStoreImpl<>(allResourceSessions, resourceSessionFactory);
  }

  @DatabaseScope
  @Provides
  static Database<JsonResourceSession> jsonDatabase(final TransactionManager transactionManager,
      final DatabaseConfiguration dbConfig, final PathBasedPool<Database<?>> sessions,
      final ResourceStore<JsonResourceSession> resourceStore, final WriteLocksRegistry writeLocks,
      final PathBasedPool<ResourceSession<?, ?>> resourceSessions) {
    return new LocalDatabase<>(transactionManager, dbConfig, sessions, resourceStore, writeLocks, resourceSessions);
  }
}
