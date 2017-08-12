package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import javax.annotation.Nonnull;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.ResourceManager;
import org.sirix.cache.BufferManager;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Manages all resource stuff.
 *
 * @author Johannes Lichtenberger
 */
public final class ResourceStore implements AutoCloseable {
	/** Central repository of all open resource managers. */
	private final ConcurrentMap<File, ResourceManager> mResourceManagers;

	/** Makes sure there is at maximum a specific number of readers per resource. */
	private final Semaphore mReadSemaphore;

	/** Makes sure there is at maximum one writer per resource. */
	private final Semaphore mWriteSempahore;

	/**
	 * Constructor.
	 *
	 * @param readSempahore makes sure there is at maximum a specific number of readers per resource
	 * @param writeSempahore makes sure there is at maximum one writer per resource.
	 */
	public ResourceStore(final Semaphore readSempahore, final Semaphore writeSemaphore) {
		mResourceManagers = new ConcurrentHashMap<>();
		mReadSemaphore = checkNotNull(readSempahore);
		mWriteSempahore = checkNotNull(writeSemaphore);
	}

	/**
	 * Open a resource, that is get an instance of a {@link ResourceManager} in order to read/write
	 * from the resource.
	 *
	 * @param database The database.
	 * @param resourceConfig The resource configuration.
	 * @param resourceManagerConfig The resource manager configuration.
	 * @param bufferManager The buffer manager.
	 * @param resourceFile The resource to open.
	 * @return A resource manager.
	 */
	public ResourceManager openResource(final @Nonnull DatabaseImpl database,
			final @Nonnull ResourceConfiguration resourceConfig,
			final @Nonnull ResourceManagerConfiguration resourceManagerConfig,
			final @Nonnull BufferManager bufferManager, final @Nonnull File resourceFile) {
		checkNotNull(database);
		checkNotNull(resourceConfig);
		return mResourceManagers.computeIfAbsent(resourceFile, k -> {
			final Storage storage = StorageType.getStorage(resourceConfig);
			final UberPage uberPage;

			if (storage.exists()) {
				try (final Reader reader = storage.createReader()) {
					final PageReference firstRef = reader.readUberPageReference();
					if (firstRef.getPage() == null) {
						uberPage = (UberPage) reader.read(firstRef.getKey(), null);
					} else {
						uberPage = (UberPage) firstRef.getPage();
					}
				}
			} else {
				// Bootstrap uber page and make sure there already is a root node.
				uberPage = new UberPage();
			}

			final ResourceManager resourceManager = new XdmResourceManager(database, this, resourceConfig,
					resourceManagerConfig, bufferManager, resourceFile,
					StorageType.getStorage(resourceConfig), uberPage, mReadSemaphore, mWriteSempahore);
			Databases.putResourceManager(resourceFile, resourceManager);
			return resourceManager;
		});
	}

	public boolean hasOpenResourceManager(File resourceFile) {
		checkNotNull(resourceFile);
		return mResourceManagers.containsKey(resourceFile);
	}

	public ResourceManager getOpenResourceManager(File resourceFile) {
		checkNotNull(resourceFile);
		return mResourceManagers.get(resourceFile);
	}

	@Override
	public void close() {
		mResourceManagers.forEach((resourceName, resourceMgr) -> resourceMgr.close());
	}

	public boolean closeResource(File resourceFile) {
		final ResourceManager manager = mResourceManagers.remove(resourceFile);
		Databases.removeResourceManager(resourceFile, manager);
		return manager != null;
	}
}
