package org.sirix.access.json;

import dagger.Module;
import dagger.Provides;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.LocalDatabase;
import org.sirix.access.LocalDatabaseModule;
import org.sirix.access.PathBasedPool;
import org.sirix.access.ResourceManagerFactory;
import org.sirix.access.ResourceStore;
import org.sirix.access.ResourceStoreImpl;
import org.sirix.access.SubComponentResourceManagerFactory;
import org.sirix.access.WriteLocksRegistry;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.TransactionManager;
import org.sirix.api.json.JsonResourceManager;
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
    static ResourceManagerFactory<JsonResourceManager> resourceManagerFactory(
            final Provider<JsonResourceManagerComponent.Builder> subComponentBuilder) {

        return new SubComponentResourceManagerFactory<>(subComponentBuilder);
    }

    @DatabaseScope
    @Provides
    static ResourceStore<JsonResourceManager> jsonResourceManager(
            final PathBasedPool<ResourceManager<?, ?>> allResourceManagers,
            final ResourceManagerFactory<JsonResourceManager> resourceManagerFactory) {
        return new ResourceStoreImpl<>(allResourceManagers, resourceManagerFactory);
    }

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
