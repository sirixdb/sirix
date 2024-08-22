package io.sirix.access;

import dagger.Module;
import dagger.Provides;
import io.sirix.dagger.ResourceSessionScope;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;

import java.util.concurrent.Semaphore;

/**
 * A module with all the common bindings between resource sessions.
 *
 * @author Joao Sousa
 */
@Module
public interface ResourceSessionModule {

	@Provides
	@ResourceSessionScope
	static IOStorage ioStorage(final ResourceConfiguration resourceConfiguration) {
		return StorageType.getStorage(resourceConfiguration);
	}

	@Provides
	@ResourceSessionScope
	static Semaphore writeLock(final WriteLocksRegistry registry, final ResourceConfiguration resourceConfiguration) {
		return registry.getWriteLock(resourceConfiguration.getResource());
	}

	@Provides
	@ResourceSessionScope
	static UberPage rootPage(final IOStorage storage) {
		final UberPage uberPage;
		if (storage.exists()) {
			try (final Reader reader = storage.createReader()) {
				final PageReference firstRef = reader.readUberPageReference();
				if (firstRef.getPage() == null) {
					uberPage = (UberPage) reader.read(firstRef, null);
				} else {
					uberPage = (UberPage) firstRef.getPage();
				}
			}
		} else {
			// Bootstrap uber page and make sure there already is a root node.
			uberPage = new UberPage();
		}
		return uberPage;
	}

}
