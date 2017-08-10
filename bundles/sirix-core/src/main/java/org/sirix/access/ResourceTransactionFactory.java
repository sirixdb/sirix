package org.sirix.access;

import java.io.File;

import javax.annotation.Nonnull;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Session;
import org.sirix.cache.BufferManager;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.StorageType;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

public final class SessionFactory {
	public SessionFactory() {
	}

	public Session createSession(final DatabaseImpl database,
			final @Nonnull ResourceConfiguration resourceConfig,
			final @Nonnull SessionConfiguration sessionConfig,
			final @Nonnull BufferManager bufferManager,
			final @Nonnull File resourceFile) {
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

		final Session session = new SessionImpl(database, resourceConfig,
				sessionConfig, bufferManager, resourceFile,
				StorageType.getStorage(resourceConfig), uberPage);

		return session;
	}
}
