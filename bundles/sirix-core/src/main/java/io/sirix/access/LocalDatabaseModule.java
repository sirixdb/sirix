package io.sirix.access;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.sirix.access.trx.TransactionManagerImpl;
import io.sirix.api.Database;
import io.sirix.api.TransactionManager;
import io.sirix.dagger.DatabaseName;
import io.sirix.dagger.DatabaseScope;

/**
 * A module with common dependencies to all {@link Database} subcomponents.
 *
 * @author Joao Sousa
 */
@Module
public interface LocalDatabaseModule {

  @Provides
  @DatabaseScope
  @DatabaseName
  static String databaseName(final DatabaseConfiguration configuration) {
    return configuration.getDatabaseName();
  }

  @Provides
  @DatabaseScope
  static DatabaseType databaseType(final DatabaseConfiguration configuration) {

    return configuration.getDatabaseType();
  }

  @Binds
  @DatabaseScope
  TransactionManager transactionManager(TransactionManagerImpl transactionManager);

}
