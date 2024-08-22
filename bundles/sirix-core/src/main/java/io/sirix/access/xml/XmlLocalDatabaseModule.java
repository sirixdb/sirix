package io.sirix.access.xml;

import dagger.Module;
import dagger.Provides;
import io.sirix.access.*;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.TransactionManager;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.dagger.DatabaseScope;

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
