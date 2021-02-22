package org.sirix.access.json;

import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.LocalDatabase;
import org.sirix.access.LocalDatabaseModule;
import org.sirix.access.PathBasedPool;
import org.sirix.access.ResourceStore;
import org.sirix.access.WriteLocksRegistry;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.TransactionManager;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.dagger.DatabaseScope;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;

/**
 * TODO: Class JsonLocalDatabaseModule's description.
 *
 * @author Joao Sousa
 */
@Module(includes = LocalDatabaseModule.class)
public interface JsonLocalDatabaseModule {

    @DatabaseScope
    @Binds
    ResourceStore<JsonResourceManager> jsonResourceManager(JsonResourceStore jsonResourceStore);

    @DatabaseScope
    @Provides
    static Database<JsonResourceManager> jsonDatabase(final TransactionManager transactionManager,
                                                      final DatabaseConfiguration dbConfig,
                                                      final PathBasedPool<Database<?>> sessions,
                                                      final ResourceStore<JsonResourceManager> resourceStore,
                                                      final WriteLocksRegistry writeLocks,
                                                      final PathBasedPool<ResourceManager<?, ?>> resourceManagers) {

        return new LocalDatabase<>(
                transactionManager,
                dbConfig,
                sessions,
                resourceStore,
                writeLocks,
                resourceManagers
        );
    }
}
