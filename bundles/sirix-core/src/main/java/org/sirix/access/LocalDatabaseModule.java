package org.sirix.access;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import org.sirix.access.trx.TransactionManagerImpl;
import org.sirix.api.TransactionManager;
import org.sirix.dagger.DatabaseName;
import org.sirix.dagger.DatabaseScope;

/**
 * TODO: Class LocalDatabaseModule's description.
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
