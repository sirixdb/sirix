package org.sirix.access.xml;

import dagger.Module;
import dagger.Provides;
import org.sirix.access.*;
import org.sirix.api.Database;
import org.sirix.api.ResourceSession;
import org.sirix.api.TransactionManager;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.dagger.DatabaseScope;

import javax.inject.Provider;

/**
 * The module for {@link XmlLocalDatabaseComponent}.
 *
 * @author Joao Sousa
 */
@Module(includes = LocalDatabaseModule.class)
public interface XmlLocalDatabaseModule {

  @DatabaseScope
  @Provides
  static ResourceSessionFactory<XmlResourceSession> resourceManagerFactory(
      final Provider<XmlResourceManagerComponent.Builder> subComponentBuilder) {
    return new SubComponentResourceSessionFactory<>(subComponentBuilder);
  }

  @DatabaseScope
  @Provides
  static Database<XmlResourceSession> xmlDatabase(final TransactionManager transactionManager,
      final DatabaseConfiguration dbConfig, final PathBasedPool<Database<?>> sessions,
      final ResourceStore<XmlResourceSession> resourceStore, final WriteLocksRegistry writeLocks,
      final PathBasedPool<ResourceSession<?, ?>> resourceManagers) {
    return new LocalDatabase<>(transactionManager, dbConfig, sessions, resourceStore, writeLocks, resourceManagers);
  }

  @DatabaseScope
  @Provides
  static ResourceStore<XmlResourceSession> xmlResourceManager(
      final PathBasedPool<ResourceSession<?, ?>> allResourceManagers,
      final ResourceSessionFactory<XmlResourceSession> resourceSessionFactory) {
    return new ResourceStoreImpl<>(allResourceManagers, resourceSessionFactory);
  }
}
